/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.malhar.lib.io.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.apps.ingestion.Application.Scheme;
import com.datatorrent.apps.ingestion.common.IngestionUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.netlet.util.DTThrowable;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Input operator that scans a directory for files and splits a file into blocks.<br/>
 * The operator emits block metadata and file metadata.<br/>
 *
 * The file system/directory space should be different for different partitions of file splitter.
 * The scanning of
 *
 * @displayName File Splitter
 * @category Input
 * @since 2.0.0
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class FileSplitter implements InputOperator
{
  protected Scheme inputScheme;
  protected Long blockSize;
  private int sequenceNo;

  /**
   * This is a threshold on the no. of blocks emitted per window. A lot of blocks emitted
   * per window can overwhelm the downstream operators. This setting helps to control that.
   */
  @Min(1)
  protected int blocksThreshold;

  protected transient long blockCount;

  protected Iterator<FileBlockMetadata> blockMetadataIterator;

  @NotNull
  protected TimeBasedDirectoryScanner scanner;

  @NotNull
  protected IdempotentStorageManager idempotentStorageManager;

  @NotNull
  protected final transient LinkedList<FileInfo> currentWindowRecoveryState;

  protected transient FileSystem fs;
  protected transient int operatorId;
  protected transient Context.OperatorContext context;
  protected transient long currentWindowId;

  protected final BasicCounters<MutableLong> fileCounters;

  public final transient DefaultOutputPort<FileMetadata> filesMetadataOutput = new DefaultOutputPort<FileMetadata>();
  public final transient DefaultOutputPort<FileBlockMetadata> blocksMetadataOutput = new DefaultOutputPort<FileBlockMetadata>();

  public FileSplitter()
  {
    currentWindowRecoveryState = Lists.newLinkedList();
    fileCounters = new BasicCounters<MutableLong>(MutableLong.class);
    idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
    scanner = new TimeBasedDirectoryScanner();
    blocksThreshold = Integer.MAX_VALUE;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    Preconditions.checkArgument(!scanner.files.isEmpty(), "empty files");
    Preconditions.checkArgument(blockSize == null || blockSize > 0, "invalid block size");

    operatorId = context.getId();
    this.context = context;

    fileCounters.setCounter(Counters.PROCESSED_FILES, new MutableLong());
    idempotentStorageManager.setup(context);

    try {
      fs = scanner.getFSInstance();
    }
    catch (IOException e) {
      throw new RuntimeException("creating fs", e);
    }

    if (blockSize == null) {
      blockSize = fs.getDefaultBlockSize(new Path(scanner.files.iterator().next()));
    }

    if (context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) < idempotentStorageManager.getLargestRecoveryWindow()) {
      blockMetadataIterator = null;
    }
    else {
      //don't setup scanner while recovery
      scanner.setup(context);
    }
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  @Override
  public void teardown()
  {
    try {
      scanner.teardown();
    }
    catch (Throwable t) {
      DTThrowable.rethrow(t);
    }
    finally {
      try {
        fs.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    blockCount = 0;
    currentWindowId = windowId;
    if (windowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      replay(windowId);
    }
  }

  protected void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      LinkedList<FileInfo> recoveredData = (LinkedList<FileInfo>) idempotentStorageManager.load(operatorId,
        windowId);
      if (recoveredData == null) {
        //This could happen when there are multiple physical instances and one of them is ahead in processing windows.
        return;
      }
      if (blockMetadataIterator != null) {
        emitBlockMetadata();
      }
      for (FileInfo info : recoveredData) {
        if (info.directoryPath != null) {
          Map<String, Long> lastModifiedTimeMap = scanner.getLastModifiedTimeMap(info.directoryPath);
          lastModifiedTimeMap.put(info.directoryPath, info.modifiedTime);
        }
        else { //no directory
          Map<String, Long> lastModifiedTimeMap = scanner.getLastModifiedTimeMap(TimeBasedDirectoryScanner.DEDUP_TIME_STAMPS);
          lastModifiedTimeMap.put(info.relativeFilePath, info.modifiedTime);
        }

        FileMetadata fileMetadata = buildFileMetadata(info);
        fileCounters.getCounter(Counters.PROCESSED_FILES).increment();
        filesMetadataOutput.emit(fileMetadata);
        blockMetadataIterator = new BlockMetadataIterator(this, fileMetadata, blockSize);

        if (!emitBlockMetadata()) {
          break;
        }
      }

      if (windowId == idempotentStorageManager.getLargestRecoveryWindow()) {
        scanner.setup(context);
      }
    }
    catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      return;
    }

    Throwable throwable;
    if ((throwable = scanner.atomicThrowable.get()) != null) {
      DTThrowable.rethrow(throwable);
    }
    if (blockMetadataIterator != null && blockCount < blocksThreshold) {
      emitBlockMetadata();
    }

    FileInfo fileInfo;
    while (blockCount < blocksThreshold && (fileInfo = scanner.pollFile()) != null) {
      currentWindowRecoveryState.add(fileInfo);
      try {
        FileMetadata fileMetadata = buildFileMetadata(fileInfo);
        filesMetadataOutput.emit(fileMetadata);
        fileCounters.getCounter(Counters.PROCESSED_FILES).increment();
        if (!fileMetadata.isDirectory()) {
          blockMetadataIterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
          if (!emitBlockMetadata()) {
            // block threshold reached
            break;
          }
        }

        if (fileInfo.lastFileOfScan) {
          break;
        }
      }
      catch (IOException e) {
        throw new RuntimeException("creating metadata", e);
      }
    }
  }

  @Override
  public void endWindow()
  {
    if (currentWindowId > idempotentStorageManager.getLargestRecoveryWindow()) {
      try {
        idempotentStorageManager.save(currentWindowRecoveryState, operatorId, currentWindowId);
      }
      catch (IOException e) {
        throw new RuntimeException("saving recovery", e);
      }
    }
    currentWindowRecoveryState.clear();
    context.setCounters(fileCounters);
  }

  /**
   * @return true if all the blocks were emitted; false otherwise
   */
  protected boolean emitBlockMetadata()
  {
    while (blockMetadataIterator.hasNext()) {
      if (blockCount++ < blocksThreshold) {
        this.blocksMetadataOutput.emit(blockMetadataIterator.next());
      }
      else {
        return false;
      }
    }
    blockMetadataIterator = null;
    return true;
  }

  /**
   * Can be overridden for creating block metadata of a type that extends {@link FileBlockMetadata}
   */
  protected FileBlockMetadata createBlockMetadata(long pos, long lengthOfFileInBlock, int blockNumber,
                                                  FileMetadata fileMetadata, boolean isLast)
  {
    return new FileBlockMetadata(fileMetadata.getFilePath(), fileMetadata.getBlockIds()[blockNumber - 1], pos,
      lengthOfFileInBlock, isLast, blockNumber == 1 ? -1 : fileMetadata.getBlockIds()[blockNumber - 2], inputScheme);

  }

  /**
   * Creates file-metadata and populates no. of blocks in the metadata.
   *
   * @param fileInfo file information
   * @return file-metadata
   * @throws IOException
   */
  protected FileMetadata buildFileMetadata(FileInfo fileInfo) throws IOException
  {
    String filePathStr = fileInfo.getFilePath();
    LOG.debug("file {}", filePathStr);
    FileMetadata fileMetadata = new FileMetadata(filePathStr);
    Path path = new Path(filePathStr);

    fileMetadata.setFileName(path.getName());

    FileStatus status = fs.getFileStatus(path);
    fileMetadata.setDirectory(status.isDirectory());
    fileMetadata.setFileLength(status.getLen());

    if (!status.isDirectory()) {
      int noOfBlocks = (int) ((status.getLen() / blockSize) + (((status.getLen() % blockSize) == 0) ? 0 : 1));
      if (fileMetadata.getDataOffset() >= status.getLen()) {
        noOfBlocks = 0;
      }
      fileMetadata.setNumberOfBlocks(noOfBlocks);
      populateBlockIds(fileMetadata);
    }
    return fileMetadata;
  }

  protected void populateBlockIds(FileMetadata fileMetadata)
  {
    // block ids are 32 bits of operatorId | 32 bits of sequence number
    long[] blockIds = new long[fileMetadata.getNumberOfBlocks()];
    long longLeftSide = ((long) operatorId) << 32;
    for (int i = 0; i < fileMetadata.getNumberOfBlocks(); i++) {
      blockIds[i] = longLeftSide | sequenceNo++ & 0xFFFFFFFFL;
    }
    fileMetadata.setBlockIds(blockIds);
  }

  public void setBlockSize(Long blockSize)
  {
    this.blockSize = blockSize;
  }

  public Long getBlockSize()
  {
    return blockSize;
  }

  public void setBlocksThreshold(int threshold)
  {
    this.blocksThreshold = threshold;
  }

  public int getBlocksThreshold()
  {
    return blocksThreshold;
  }

  public void setScanner(TimeBasedDirectoryScanner scanner)
  {
    this.scanner = scanner;
  }

  public TimeBasedDirectoryScanner getScanner()
  {
    return this.scanner;
  }

  public void setIdempotentStorageManager(IdempotentStorageManager idempotentStorageManager)
  {
    this.idempotentStorageManager = idempotentStorageManager;
  }

  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return this.idempotentStorageManager;
  }
  
  /**
   * @return the inputScheme
   */
  public Scheme getInputScheme()
  {
    return inputScheme;
  }

  /**
   * @param inputScheme the inputScheme to set
   */
  public void setInputScheme(Scheme inputScheme)
  {
    this.inputScheme = inputScheme;
  }


  /**
   * An {@link Iterator} for Block-Metadatas of a file.
   */
  public static class BlockMetadataIterator implements Iterator<FileBlockMetadata>
  {
    private final FileMetadata fileMetadata;
    private final long blockSize;

    private long pos;
    private int blockNumber;

    private final FileSplitter splitter;

    protected BlockMetadataIterator()
    {
      //for kryo
      fileMetadata = null;
      blockSize = -1;
      splitter = null;
    }

    public BlockMetadataIterator(FileSplitter splitter, FileMetadata fileMetadata, long blockSize)
    {
      this.splitter = splitter;
      this.fileMetadata = fileMetadata;
      this.blockSize = blockSize;
      this.pos = fileMetadata.getDataOffset();
      this.blockNumber = 0;
    }

    @Override
    public boolean hasNext()
    {
      return pos < fileMetadata.getFileLength();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public FileBlockMetadata next()
    {
      long length;
      while ((length = blockSize * ++blockNumber) <= pos) {
      }
      boolean isLast = length >= fileMetadata.getFileLength();
      long lengthOfFileInBlock = isLast ? fileMetadata.getFileLength() : length;
      FileBlockMetadata fileBlock = splitter.createBlockMetadata(pos, lengthOfFileInBlock, blockNumber, fileMetadata, isLast);
      pos = lengthOfFileInBlock;
      return fileBlock;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException("remove not supported");
    }
  }

  /**
   * Represents the file metadata - file path, name, no. of blocks, etc.
   */
  public static class FileMetadata
  {
    @NotNull
    private String filePath;
    private String fileName;
    private int numberOfBlocks;
    private long dataOffset;
    private long fileLength;
    private long discoverTime;
    private long completionTime;
    private long[] blockIds;
    private boolean isDirectory;

    @SuppressWarnings("unused")
    protected FileMetadata()
    {
      //for kryo
      filePath = null;
      discoverTime = System.currentTimeMillis();
      completionTime = 0;
    }

    /**
     * Constructs file metadata
     *
     * @param filePath file path
     */
    public FileMetadata(@NotNull String filePath)
    {
      this.filePath = filePath;
      discoverTime = System.currentTimeMillis();
      completionTime = 0;
    }

    /**
     * Returns time of completion. 
     */
    public long getCompletionTime()
    {
      return completionTime;
    }

    /**
     * Sets time of completion. 
     * @param completionTime
     */
    public void setCompletionTime(long completionTime)
    {
      this.completionTime = completionTime;
    }

    /**
     * Returns the total number of blocks.
     */
    public int getNumberOfBlocks()
    {
      return numberOfBlocks;
    }

    /**
     * Sets the total number of blocks.
     */
    public void setNumberOfBlocks(int numberOfBlocks)
    {
      this.numberOfBlocks = numberOfBlocks;
    }

    /**
     * Returns the file name.
     */
    public String getFileName()
    {
      return fileName;
    }

    /**
     * Sets the file name.
     */
    public void setFileName(String fileName)
    {
      this.fileName = fileName;
    }

    /**
     * Sets the file path.
     */
    public void setFilePath(String filePath)
    {
      this.filePath = filePath;
    }

    /**
     * Returns the file path.
     */
    public String getFilePath()
    {
      return filePath;
    }

    /**
     * Returns the data offset.
     */
    public long getDataOffset()
    {
      return dataOffset;
    }

    /**
     * Sets the data offset.
     */
    public void setDataOffset(long offset)
    {
      this.dataOffset = offset;
    }

    /**
     * Returns the file length.
     */
    public long getFileLength()
    {
      return fileLength;
    }

    /**
     * Sets the file length.
     */
    public void setFileLength(long fileLength)
    {
      this.fileLength = fileLength;
    }

    /**
     * Returns the file discover time.
     */
    public long getDiscoverTime()
    {
      return discoverTime;
    }

    /**
     * Sets the discover time.
     */
    public void setDiscoverTime(long discoverTime)
    {
      this.discoverTime = discoverTime;
    }

    /**
     * Returns the block ids associated with the file.
     */
    public long[] getBlockIds()
    {
      return blockIds;
    }

    /**
     * Sets the blocks ids of the file.
     */
    public void setBlockIds(long[] blockIds)
    {
      this.blockIds = blockIds;
    }

    /**
     * Sets whether the file metadata is a directory.
     */
    public void setDirectory(boolean isDirectory)
    {
      this.isDirectory = isDirectory;
    }

    /**
     * @return true if it is a directory; false otherwise.
     */
    public boolean isDirectory()
    {
      return isDirectory;
    }
  }

  public static class TimeBasedDirectoryScanner implements Component<Context.OperatorContext>, Runnable
  {
    private static long DEF_SCAN_INTERVAL_MILLIS = 5000;

    protected boolean recursive;

    protected transient volatile boolean trigger;
    private boolean dedup;

    @NotNull
    protected final Map<String,  Map<String, Long>> inputDirTolastModifiedTimes;
    protected static final String DEDUP_TIME_STAMPS = "Dedup timestamps map";

    @NotNull
    protected final Set<String> files;

    @Min(0)
    protected long scanIntervalMillis;

    private String filePatternRegularExp;

    protected transient long lastScanMillis;
    protected long noOfDiscoveredFilesInThisScan;
    protected transient FileSystem fs;
    protected final transient LinkedBlockingDeque<FileInfo> discoveredFiles;
    protected final transient ExecutorService scanService;
    public final transient AtomicReference<Throwable> atomicThrowable;

    protected transient volatile boolean running;
    protected final transient Set<String> ignoredFiles;
    protected transient Pattern regex;
    protected transient long sleepMillis;

    public TimeBasedDirectoryScanner()
    {
      inputDirTolastModifiedTimes = Maps.newConcurrentMap();
      recursive = true;
      dedup = false;
      scanIntervalMillis = DEF_SCAN_INTERVAL_MILLIS;
      files =  new ConcurrentSkipListSet<String>();
      scanService = Executors.newSingleThreadExecutor();
      discoveredFiles = new LinkedBlockingDeque<FileInfo>();
      atomicThrowable = new AtomicReference<Throwable>();
      ignoredFiles = Sets.newHashSet();
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      sleepMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
      if (filePatternRegularExp != null) {
        regex = Pattern.compile(filePatternRegularExp);
      }
      try {
        fs = getFSInstance();
      }
      catch (IOException e) {
        throw new RuntimeException("opening fs", e);
      }
      scanService.submit(this);
    }

    @Override
    public void teardown()
    {
      running = false;
      scanService.shutdownNow();
      try {
        fs.close();
      }
      catch (IOException e) {
        throw new RuntimeException("closing fs", e);
      }
    }

    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.newInstance(new Path(files.iterator().next()).toUri(), new Configuration());
    }

    protected Path createPathObject(String aFile)
    {
      return new Path(aFile);
    }

    @Override
    public void run()
    {
      running = true;
      Map<String, Long> lastModifiedTimesForInputDir;
      try {
        while (running) {
          if (trigger || (System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis)) {
            trigger = false;
            noOfDiscoveredFilesInThisScan = 0;
            for (String afile : files) {
              String timeStampMapKey = DEDUP_TIME_STAMPS;
              if (!dedup) {
                timeStampMapKey = afile;
              }
              lastModifiedTimesForInputDir = getLastModifiedTimeMap(timeStampMapKey);
              scan(createPathObject(afile), null, lastModifiedTimesForInputDir);
            }
            scanComplete();
          }
          else {
            Thread.sleep(sleepMillis);
          }
        }
      }
      catch (Throwable throwable) {
        LOG.error("service", throwable);
        running = false;
        atomicThrowable.set(throwable);
        DTThrowable.rethrow(throwable);
      }
    }

    /**
     * Operations that need to be done once a scan is complete.
     */
    protected void scanComplete()
    {
      LOG.debug("scan complete {}", lastScanMillis);
      FileInfo fileInfo = discoveredFiles.peekLast();
      if (fileInfo != null) {
        fileInfo.lastFileOfScan = true;
      }
      lastScanMillis = System.currentTimeMillis();
    }

    protected void scan(@NotNull Path filePath, Path rootPath, Map<String, Long> lastModifiedTimesForInputDir)
    {
      try {
        FileStatus parentStatus = fs.getFileStatus(filePath);
        String parentPathStr = filePath.toUri().getPath();

        LOG.debug("scan {}", parentPathStr);
        LOG.debug("scan {}", filePath.toUri().getPath());

        FileStatus[] childStatuses = fs.listStatus(filePath);

        if (childStatuses.length == 0 && rootPath == null &&  lastModifiedTimesForInputDir.get(parentPathStr) == null) { // empty input directory copy as is
          FileInfo info = new FileInfo(null, filePath.toString(), parentStatus.getModificationTime());
          discoveredFiles.add(info);
          ++noOfDiscoveredFilesInThisScan;
          lastModifiedTimesForInputDir.put(parentPathStr, parentStatus.getModificationTime());
          return;
        }

        for (FileStatus childStatus : childStatuses) {
          Path childPath = childStatus.getPath();
          String childPathStr = childPath.toUri().getPath();

          if (childStatus.isSymlink()) {
            ignoredFiles.add(childPathStr);
          } else if (childStatus.isDirectory() && recursive) {
            addToDiscoveredFiles(rootPath, parentStatus, parentPathStr, childStatus, lastModifiedTimesForInputDir);
            scan(childPath, rootPath == null ? parentStatus.getPath() : rootPath, lastModifiedTimesForInputDir);
          } else if (acceptFile(childPathStr)) {
            addToDiscoveredFiles(rootPath, parentStatus, parentPathStr, childStatus, lastModifiedTimesForInputDir);
          } else {
            // don't look at it again
            ignoredFiles.add(childPathStr);
          }
        }
      }
      catch (FileNotFoundException fnf) {
        LOG.warn("Failed to list directory {}", filePath, fnf);
      }
      catch (IOException e) {
        throw new RuntimeException("listing files", e);
      }
    }
    
    private void addToDiscoveredFiles(Path rootPath, FileStatus parentStatus, String parentPathStr, FileStatus childStatus, Map<String, Long> lastModifiedTimesForInputDir) throws IOException{
      Path childPath = childStatus.getPath();
      String childPathStr = childPath.toUri().getPath();
      // Directory by now is scanned forcibly. Now check for whether file/directory needs to be added to discoveredFiles.
      Long oldModificationTime = lastModifiedTimesForInputDir.get(childPathStr);
      lastModifiedTimesForInputDir.put(childPathStr, childStatus.getModificationTime());

      if (skipFile(childPath, childStatus.getModificationTime(), oldModificationTime) || // Skip dir or file if no timestamp modification
          (childStatus.isDirectory() && (oldModificationTime != null))) { // If timestamp modified but if its a directory and already present in map, then skip.
        return;
      }

      if (ignoredFiles.contains(childPathStr)) {
        return;
      }

      FileInfo info;
      if(rootPath == null) {
       info =parentStatus.isDirectory() ?
          new FileInfo(parentPathStr, childPath.getName(), parentStatus.getModificationTime()) :
          new FileInfo(null, childPathStr, parentStatus.getModificationTime());
      }
      else {
        URI relativeChildURI = rootPath.toUri().relativize(childPath.toUri());
        info = new FileInfo(rootPath.toUri().getPath(), relativeChildURI.getPath(),
          parentStatus.getModificationTime());
      }

      discoveredFiles.add(info);
      ++noOfDiscoveredFilesInThisScan;
      LOG.debug("Discovered path is : {}", childPathStr);
    }

    private Map<String, Long> getLastModifiedTimeMap(String key)
    {
      if(inputDirTolastModifiedTimes.get(key) == null) {
        Map<String, Long> modifiedTimeMap = Maps.newHashMap();
        inputDirTolastModifiedTimes.put(key, modifiedTimeMap);
      }
      return inputDirTolastModifiedTimes.get(key);
    }
    
    public long getNoOfDiscoveredFilesInThisScan()
    {
      return noOfDiscoveredFilesInThisScan;
    }

    /**
     * Skips file/directory based on their modification time.<br/>
     *
     * @param path                 file path
     * @param modificationTime     modification time
     * @param lastModificationTime last cached directory modification time
     * @return true to skip; false otherwise.
     * @throws IOException
     */
    protected boolean skipFile(@SuppressWarnings("unused") @NotNull Path path, @NotNull Long modificationTime,
                               Long lastModificationTime) throws IOException
    {
      return (!(lastModificationTime == null || modificationTime > lastModificationTime));
    }

    /**
     * Accepts file which match a regular pattern.
     *
     * @param filePathStr file path
     * @return true if the path matches the pattern; false otherwise;
     */
    protected boolean acceptFile(String filePathStr)
    {
      String fileName = new Path(filePathStr).getName();
      if (regex != null) {
        Matcher matcher = regex.matcher(fileName);
        if (!matcher.matches()) {
          return false;
        }
      }
      return true;
    }

    public FileInfo pollFile()
    {
      return discoveredFiles.poll();
    }

    /**
     * @return regular expression
     */
    public String getFilePatternRegularExp()
    {
      return filePatternRegularExp;
    }

    /**
     * Sets the regular expression for files.
     *
     * @param filePatternRegexp regular expression
     */
    public void setFilePatternRegularExp(String filePatternRegexp)
    {
      this.filePatternRegularExp = filePatternRegexp;
      this.regex = Pattern.compile(filePatternRegularExp);
    }

    /**
     * Sets the files to be scanned.
     *
     * @param files files
     */
    public void setFiles(String files)
    {
    	Iterables.addAll(this.files, Splitter.on(",").omitEmptyStrings().split(IngestionUtils.convertSchemeToLowerCase(files)));
    }

    /**
     * @return files to be scanned.
     */
    public String getFiles()
    {
      return Joiner.on(",").join(this.files);
    }

    /**
     * Sets whether scan will be recursive.
     *
     * @param recursive true if recursive; false otherwise.
     */
    public void setRecursive(boolean recursive)
    {
      this.recursive = recursive;
    }

    /**
     * @return true if recursive; false otherwise.
     */
    public boolean isRecursive()
    {
      return this.recursive;
    }

    /**
     * Sets the trigger which will initiate scan.
     *
     * @param trigger
     */
    public void setTrigger(boolean trigger)
    {
      this.trigger = trigger;
    }

    /**
     * Returns the value of trigger.
     *
     * @return trigger
     */
    public boolean isTrigger()
    {
      return this.trigger;
    }
    
    /**
     * Sets weather to dedup scanning of input files
     * 
     * @param dedup
     */
    public void setDedup(boolean dedup)
    {
      this.dedup = dedup;
    }

    public boolean isDedup()
    {
      return dedup;
    }

    /**
     * Returns the frequency with which new files are scanned for in milliseconds.
     *
     * @return The scan interval in milliseconds.
     */
    public long getScanIntervalMillis()
    {
      return scanIntervalMillis;
    }

    /**
     * Sets the frequency with which new files are scanned for in milliseconds.
     *
     * @param scanIntervalMillis The scan interval in milliseconds.
     */
    public void setScanIntervalMillis(long scanIntervalMillis)
    {
      this.scanIntervalMillis = scanIntervalMillis;
    }
  }

  /**
   * A class that represents the file discovered by time-based scanner.
   */
  public static class FileInfo
  {
    protected final String directoryPath;
    protected final String relativeFilePath;
    protected final long modifiedTime;
    public transient boolean lastFileOfScan;

    private FileInfo()
    {
      directoryPath = null;
      relativeFilePath = null;
      modifiedTime = -1;
    }

    protected FileInfo(@Nullable String directoryPath, @NotNull String relativeFilePath, long modifiedTime)
    {
      this.directoryPath = directoryPath;
      this.relativeFilePath = relativeFilePath;
      this.modifiedTime = modifiedTime;
    }

    /**
     * @return directory path
     */
    public String getDirectoryPath()
    {
      return directoryPath;
    }

    /**
     * @return path relative to directory
     */
    public String getRelativeFilePath()
    {
      return relativeFilePath;
    }

    /**
     * @return full path of the file
     */
    public String getFilePath()
    {
      if (directoryPath == null) {
        return relativeFilePath;
      }
      return new Path(directoryPath, relativeFilePath).toUri().getPath();
    }

    public boolean isLastFileOfScan()
    {
      return lastFileOfScan;
    }
  }

  public static enum Counters
  {
    PROCESSED_FILES
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSplitter.class);
}
