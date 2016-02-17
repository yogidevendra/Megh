package com.datatorrent.apps.ingestion.io.input;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.apps.ingestion.Application;
import com.datatorrent.apps.ingestion.TrackerEvent;
import com.datatorrent.apps.ingestion.TrackerEvent.TrackerEventDetails;
import com.datatorrent.apps.ingestion.TrackerEvent.TrackerEventType;
import com.datatorrent.apps.ingestion.io.BandwidthLimitingOperator;
import com.datatorrent.apps.ingestion.io.ftp.DTFTPFileSystem;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputBlock;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputFileBlockMetaData;
import com.datatorrent.apps.ingestion.io.s3.DTS3FileSystem;
import com.datatorrent.apps.ingestion.lib.BandwidthManager;
import com.datatorrent.lib.io.IdempotentStorageManager.FSIdempotentStorageManager;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.malhar.lib.io.fs.FileSplitter;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * <p>IngestionFileSplitter class.</p>
 *
 * @since 1.0.0
 */
public class IngestionFileSplitter extends FileSplitter implements BandwidthLimitingOperator
{
  public static final String IDEMPOTENCY_RECOVERY = "idempotency";
  private boolean fastMergeEnabled = false;

  private transient String oneTimeCopyComplete;
  private transient Path oneTimeCopyCompletePath;
  private transient FileSystem appFS;
  private String compressionExtension;
  private BandwidthManager bandwidthManager;
  private FileBlockMetadata currentBlockMetadata;
  
  public final transient DefaultOutputPort<TrackerEvent> trackerOutPort = new DefaultOutputPort<TrackerEvent>();

  public IngestionFileSplitter()
  {
    super();
    scanner = new Scanner();
    ((FSIdempotentStorageManager) idempotentStorageManager).setRecoveryPath("");
    blocksThreshold = 1;
    bandwidthManager = new BandwidthManager();
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (idempotentStorageManager instanceof FSIdempotentStorageManager) {
      String recoveryPath = IDEMPOTENCY_RECOVERY;
      ((FSIdempotentStorageManager) idempotentStorageManager).setRecoveryPath(recoveryPath);
    }
    fileCounters.setCounter(PropertyCounters.THRESHOLD, new MutableLong());

    fastMergeEnabled = fastMergeEnabled && (blockSize == null);
    if (((Scanner) scanner).getIgnoreFilePatternRegularExp() == null) {
      String pathURI = scanner.getFiles().split(",")[0];
      URI inputURI = URI.create(pathURI);
      if(Application.Scheme.HDFS.toString().equalsIgnoreCase(inputURI.getScheme())) {
        ((Scanner) scanner).setIgnoreFilePatternRegularExp(".*._COPYING_");
      }
    }

    super.setup(context);
    bandwidthManager.setup(context);

    try {
      appFS = getAppFS();
    } catch (IOException e) {
      throw new RuntimeException("Unable to get FileSystem instance.", e);
    }
    oneTimeCopyComplete = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + Application.ONE_TIME_COPY_DONE_FILE;
    oneTimeCopyCompletePath = new Path(oneTimeCopyComplete);

    // override blockSize calculated in setup() to default HDFS block size to enable fast merge on HDFS
    if (fastMergeEnabled) {
      blockSize = hdfsBlockSize(context.getValue(DAGContext.APPLICATION_PATH));
    }
  }

  private FileSystem getAppFS() throws IOException
  {
    return FileSystem.newInstance(new Configuration());
  }

  @Override
  public void teardown()
  {
    super.teardown();
    bandwidthManager.teardown();
    if (appFS != null) {
      try {
        appFS.close();
      } catch (IOException e) {
        throw new RuntimeException("Unable to close application file system.", e);
      }
    }
  }

  @Override
  public void endWindow()
  {
    fileCounters.getCounter(PropertyCounters.THRESHOLD).setValue(blocksThreshold);
    super.endWindow();

    if (((Scanner) scanner).isOneTimeCopy() && ((Scanner) scanner).isFirstScanComplete() && blockMetadataIterator == null) {
      try {
        checkCompletion();
      } catch (IOException e) {
        throw new RuntimeException("Unable to check shutdown signal file.", e);
      }
    }
    
    Queue<PollingEventDetails> queue = ((Scanner) scanner).getPollingEventsQueue();
    for(PollingEventDetails eventDetails = queue.poll(); eventDetails !=null; eventDetails = queue.poll()){
      trackerOutPort.emit(new TrackerEvent(TrackerEventType.INFO, eventDetails));
    }
    
    Queue<String> skippedFilesQueue = ((Scanner) scanner).getSkippedFilesQueue();
    for(String filePath = skippedFilesQueue.poll(); filePath !=null; filePath = skippedFilesQueue.poll()){
      trackerOutPort.emit(new TrackerEvent(TrackerEventType.SKIPPED_FILE, filePath));
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
    if (blockMetadataIterator != null) {
      if(!emitBlockMetadata()) {
        return;
      }
    }

    FileInfo fileInfo;
    while ((fileInfo = scanner.pollFile()) != null) {
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
  protected boolean emitBlockMetadata()
  {
    boolean emitNext = true;
    while (emitNext) {
      if (currentBlockMetadata == null) {
        if (blockMetadataIterator.hasNext()) {
          currentBlockMetadata = blockMetadataIterator.next();
        } else {
          blockMetadataIterator = null;
          LOG.info("Done processing file.");
          break;
        }
      }
      emitNext = emitCurrentBlockMetadata();
    }
    return emitNext;
  }

  private boolean emitCurrentBlockMetadata()
  {
    long currBlockSize = currentBlockMetadata.getLength() - currentBlockMetadata.getOffset();
    if ((!bandwidthManager.isBandwidthRestricted() && blockCount < blocksThreshold) || 
        (bandwidthManager.isBandwidthRestricted() && bandwidthManager.canConsumeBandwidth())) {
      this.blocksMetadataOutput.emit(currentBlockMetadata);
      super.blockCount++;
      currentBlockMetadata = null;
      bandwidthManager.consumeBandwidth(currBlockSize);
      return true;
    }
    return false;
  }

  private void checkCompletion() throws IOException
  {
    if (appFS.exists(oneTimeCopyCompletePath)) {
      LOG.info("One time copy completed. Sending shutdown signal.");
      throw new ShutdownException();
    }
  }

  private long hdfsBlockSize(String path)
  {
    return appFS.getDefaultBlockSize(new Path(path));
  }

  @Override
  public BandwidthManager getBandwidthManager()
  {
    return bandwidthManager;
  }

  public void setBandwidthManager(BandwidthManager bandwidthManager)
  {
    this.bandwidthManager = bandwidthManager;
  }

  public static class IngestionFileMetaData extends FileSplitter.FileMetadata implements OutputFileMetaData
  {
    private String relativePath;
    private List<OutputBlock> outputBlockMetaDataList;
    private TrackerEventType completionStatus;
    private long compressionTime;
    private long outputFileSize;
    private long encryptionTime;
    
    public IngestionFileMetaData()
    {
      super();
      outputBlockMetaDataList = Lists.newArrayList();
    }

    public IngestionFileMetaData(String currentFile)
    {
      super(currentFile);
      completionStatus = TrackerEventType.DISCOVERED;
      compressionTime = 0;
      outputFileSize = 0;
      encryptionTime = 0;
    }
    
    public String getRelativePath()
    {
      return relativePath;
    }

    public void setRelativePath(String relativePath)
    {
      this.relativePath = relativePath;
    }
    
    @Override
    public String getOutputRelativePath()
    {
      return relativePath;
    }

    /* (non-Javadoc)
     * @see com.datatorrent.apps.ingestion.io.output.OutputFileMetaData#getOutputBlocksList()
     */
    @Override
    public List<OutputBlock> getOutputBlocksList()
    {
      return outputBlockMetaDataList;
    }
    
    /**
     * @param outputBlockMetaDataList the outputBlockMetaDataList to set
     */
    public void setOutputBlockMetaDataList(List<OutputBlock> outputBlockMetaDataList)
    {
      this.outputBlockMetaDataList = outputBlockMetaDataList;
    }

    public TrackerEventType getCompletionStatus()
    {
      return completionStatus;
    }

    public void setCompletionStatus(TrackerEventType completionStatus)
    {
      this.completionStatus = completionStatus;
    }

    public long getCompressionTime()
    {
      return compressionTime;
    }

    public void setCompressionTime(long compressionTime)
    {
      this.compressionTime = compressionTime;
    }

    public long getOutputFileSize()
    {
      return outputFileSize;
    }

    public void setOutputFileSize(long compressionSize)
    {
      this.outputFileSize = compressionSize;
    }

    public long getEncryptionTime()
    {
      return encryptionTime;
    }

    public void setEncryptionTime(long encryptionTime)
    {
      this.encryptionTime = encryptionTime;
    }
  }
  

  public static class Scanner extends TimeBasedDirectoryScanner
  {
    private String ignoreFilePatternRegularExp;
    private transient Pattern ignoreRegex;
    long pollingStartTime;
    private boolean oneTimeCopy;
    private boolean firstScanComplete;
    
    protected Queue<PollingEventDetails> pollingEventsQueue = Queues.newLinkedBlockingQueue();
    protected Queue<String> skippedFilesQueue = Queues.newLinkedBlockingQueue();

    @Override
    public void setup(OperatorContext context)
    {
      if (ignoreFilePatternRegularExp != null) {
        ignoreRegex = Pattern.compile(this.ignoreFilePatternRegularExp);
      }
      super.setup(context);
    }

    public String getIgnoreFilePatternRegularExp()
    {
      return ignoreFilePatternRegularExp;
    }

    public void setIgnoreFilePatternRegularExp(String ignoreFilePatternRegularExp)
    {
      this.ignoreFilePatternRegularExp = ignoreFilePatternRegularExp;
      this.ignoreRegex = Pattern.compile(ignoreFilePatternRegularExp);
    }

    @Override
    protected boolean acceptFile(String filePathStr)
    {
      boolean accepted = acceptFileName(filePathStr);
      addToskippedFilesQueue(accepted,filePathStr);
      return accepted;
    }
    
    private void addToskippedFilesQueue(boolean accepted,String filePathStr){
    //Check for new skipped files
      if(!accepted && ! ignoredFiles.contains(filePathStr)){
        skippedFilesQueue.add(filePathStr);
      }
    }
    
    protected boolean acceptFileName(String filePathStr)
    {
      boolean accepted = super.acceptFile(filePathStr);
      if (!accepted) {
        return false;
      }
      String fileName = new Path(filePathStr).getName();
      if (ignoreRegex != null) {
        Matcher matcher = ignoreRegex.matcher(fileName);
        // If matched against ignored Regex then do not accept the file.
        if (matcher.matches()) {
          return false;
        }
      }
      if (containsUnsupportedCharacters(filePathStr)) {
        return false;
      }
      return true;
    }

    private boolean containsUnsupportedCharacters(String filePathStr)
    {
      return new Path(filePathStr).toUri().getPath().contains(":");
    }

    /* (non-Javadoc)
     * @see com.datatorrent.lib.io.fs.FileSplitter.TimeBasedDirectoryScanner#scan(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path)
     */
    @Override
    protected void scan(Path filePath, Path rootPath, Map<String, Long> lastModifiedTimesForInputDir)
    {
      long scanStartTime = System.currentTimeMillis();
      super.scan(filePath, rootPath, lastModifiedTimesForInputDir);
      pollingStartTime = scanStartTime;
    }

    @Override
    protected void scanComplete()
    {
      super.scanComplete();
      if (oneTimeCopy) {
        running = false;
      }
      firstScanComplete = true;
      pollingEventsQueue.add(new PollingEventDetails(pollingStartTime, getNoOfDiscoveredFilesInThisScan()));
    }

    @Override protected Path createPathObject(String aFile)
    {
      String pathURI = files.iterator().next();
      URI inputURI = URI.create(pathURI);

      if (inputURI.getScheme().equalsIgnoreCase(Application.Scheme.FTP.toString())) {
        return new Path(StringUtils.stripStart(new Path(aFile).toUri().getPath(), "/"));
      }
      else {
        return super.createPathObject(aFile);
      }
    }

    @Override protected FileSystem getFSInstance() throws IOException
    {
      String pathURI = files.iterator().next();
      URI inputURI = URI.create(pathURI);

      if (inputURI.getScheme().equalsIgnoreCase(Application.Scheme.FTP.toString())) {
        DTFTPFileSystem fileSystem = new DTFTPFileSystem();
        String uriWithoutPath = pathURI.replaceAll(inputURI.getPath(), "");
        fileSystem.initialize(URI.create(uriWithoutPath), new Configuration());
        return fileSystem;
      } else if(inputURI.getScheme().equalsIgnoreCase(Application.Scheme.S3N.toString())) {
        DTS3FileSystem s3System = new DTS3FileSystem();
        s3System.initialize(new Path(files.iterator().next()).toUri(), new Configuration());
        return s3System;
      }
      else {
        return super.getFSInstance();
      }
    }

    /**
     * @return the oneTimeCopy
     */
    public boolean isOneTimeCopy()
    {
      return oneTimeCopy;
    }

    /**
     * @param oneTimeCopy
     *          the oneTimeCopy to set
     */
    public void setOneTimeCopy(boolean oneTimeCopy)
    {
      this.oneTimeCopy = oneTimeCopy;
    }

    /**
     * @return the firstScanComplete
     */
    public boolean isFirstScanComplete()
    {
      return firstScanComplete;
    }

    /**
     * @param firstScanComplete
     *          the firstScanComplete to set
     */
    public void setFirstScanComplete(boolean firstScanComplete)
    {
      this.firstScanComplete = firstScanComplete;
    }
   
    public Queue<PollingEventDetails> getPollingEventsQueue()
    {
      return pollingEventsQueue;
    }
    
    public Queue<String> getSkippedFilesQueue()
    {
      return skippedFilesQueue;
    }
    
  }

  @Override
  protected IngestionFileMetaData buildFileMetadata(FileInfo fileInfo) throws IOException
  {
    String filePathStr = fileInfo.getFilePath();
    LOG.debug("file {}", filePathStr);
    IngestionFileMetaData fileMetadata = new IngestionFileMetaData(filePathStr);
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

    if (fileInfo.getDirectoryPath() == null) { // Direct filename is given as input.
      fileMetadata.setRelativePath(status.getPath().getName());
    } else {
      String relativePath = getRelativePathWithFolderName(fileInfo);
      fileMetadata.setRelativePath(relativePath);
    }

    if (compressionExtension != null && !fileMetadata.isDirectory()) {
      String extension = "." + compressionExtension;
      fileMetadata.setRelativePath(fileMetadata.getRelativePath() + extension);
    }

    LOG.debug("Setting relative path as {}  for file {}", fileMetadata.getRelativePath(), filePathStr);
    
    fileMetadata.setOutputBlockMetaDataList(populateOutputFileBlockMetaData(fileMetadata));
    return fileMetadata;
  }
  
  public List<OutputBlock> populateOutputFileBlockMetaData(IngestionFileMetaData fileMetadata){
    List<OutputBlock> outputBlockMetaDataList = Lists.newArrayList();
    if(!fileMetadata.isDirectory()){
      Iterator<FileBlockMetadata> fileBlockMetadataIterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
      while(fileBlockMetadataIterator.hasNext()){
        FileBlockMetadata fmd = fileBlockMetadataIterator.next();
        OutputFileBlockMetaData outputFileBlockMetaData = new OutputFileBlockMetaData(fmd, fileMetadata.relativePath, fileBlockMetadataIterator.hasNext());
        outputBlockMetaDataList.add(outputFileBlockMetaData);
      }
    }
    return outputBlockMetaDataList;
  }


  /*
   * As folder name was given to input for copy, prefix folder name to the sub items to copy.
   */
  private String getRelativePathWithFolderName(FileInfo fileInfo)
  {
    String parentDir = new Path(fileInfo.getDirectoryPath()).getName();
    return parentDir + File.separator + fileInfo.getRelativeFilePath();
  }

  @Override
  public void setBlocksThreshold(int threshold)
  {
    LOG.debug("blocks threshold changed to {}", threshold);
    super.setBlocksThreshold(threshold);
  }

  public static enum PropertyCounters {
    THRESHOLD
  }
  
  public static class PollingEventDetails implements TrackerEventDetails{
    long startTime;
    long discoveredFilesCount;
    String pollingDescription = "Polling started at %d (%tc). %d files discovered.";
    
    /**
     * @param startTime
     * @param discoveredFilesCount
     */
    public PollingEventDetails(long startTime, long discoveredFilesCount)
    {
      super();
      this.startTime = startTime;
      this.discoveredFilesCount = discoveredFilesCount;
    }
    
    public PollingEventDetails()
    {
    	//For kryo
    }
    
    @Override
    public String getDescription()
    {
      return String.format(pollingDescription, startTime, startTime ,discoveredFilesCount);
    }
  }

  /**
   * @return the fastMergeEnabled
   */
  public boolean isFastMergeEnabled()
  {
    return fastMergeEnabled;
  }

  /**
   * @param fastMergeEnabled
   *          the fastMergeEnabled to set
   */
  public void setFastMergeEnabled(boolean fastMergeEnabled)
  {
    this.fastMergeEnabled = fastMergeEnabled;
  }

  public void setcompressionExtension(String compressionExtension)
  {
    this.compressionExtension = compressionExtension;
  }

  private static final Logger LOG = LoggerFactory.getLogger(IngestionFileSplitter.class);
  
}
