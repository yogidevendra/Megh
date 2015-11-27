package com.datatorrent.module.io.fs;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.datatorrent.lib.bandwidth.BandwidthLimitingOperator;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.ModuleBlockMetadata;
import com.datatorrent.lib.io.input.FileSplitterInput;
import com.datatorrent.lib.io.input.AbstractFileSplitter.FileMetadata;
import com.datatorrent.lib.io.input.ModuleFileSplitter;
import com.datatorrent.lib.io.input.ModuleFileSplitter.ModuleFileMetaData;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.module.io.fs.OutputFileMetaData.OutputBlock;
import com.datatorrent.module.io.fs.OutputFileMetaData.OutputFileBlockMetaData;
import com.datatorrent.module.io.fs.TrackerEvent.TrackerEventType;
import com.datatorrent.lib.io.input.AbstractFileSplitter.BlockMetadataIterator;

/**
 * <p>IngestionFileSplitter class.</p>
 *
 * @since 1.0.0
 */
public class IngestionFileSplitter extends ModuleFileSplitter implements BandwidthLimitingOperator
{
  public static class IngestionFileMetaData extends ModuleFileMetaData implements OutputFileMetaData
  {
    private String relativePath;
    private List<OutputBlock> outputBlockMetaDataList;
    private TrackerEventType completionStatus;
    private long completionTime;
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
    
    public IngestionFileMetaData(ModuleFileMetaData fileMetadata)
    {
      super(fileMetadata.getFilePath());
      setRelativePath(fileMetadata.getRelativePath());
      setFileName(fileMetadata.getFileName());
      setNumberOfBlocks(fileMetadata.getNumberOfBlocks());
      setDataOffset(fileMetadata.getDataOffset());
      setFileLength(fileMetadata.getFileLength());
      setDiscoverTime(fileMetadata.getDiscoverTime());
      setBlockIds(fileMetadata.getBlockIds());
      setDirectory(fileMetadata.isDirectory());
      setOutputBlockMetaDataList(new IngestionFileSplitter().populateOutputFileBlockMetaData(fileMetadata));
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
    
    public long getCompletionTime()
    {
      return completionTime;
    }
    
    public void setCompletionTime(long completionTime)
    {
      this.completionTime = completionTime;
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
  

  public List<OutputBlock> populateOutputFileBlockMetaData(ModuleFileMetaData fileMetadata){
    List<OutputBlock> outputBlockMetaDataList = Lists.newArrayList();
    if(!fileMetadata.isDirectory()){
      Iterator<FileBlockMetadata> fileBlockMetadataIterator = new BlockMetadataIterator(this, fileMetadata, 128000000L);
      while(fileBlockMetadataIterator.hasNext()){
        FileBlockMetadata fmd = fileBlockMetadataIterator.next();
        OutputFileBlockMetaData outputFileBlockMetaData = new OutputFileBlockMetaData(fmd, fileMetadata.getRelativePath(), fileBlockMetadataIterator.hasNext());
        outputBlockMetaDataList.add(outputFileBlockMetaData);
      }
    }
    return outputBlockMetaDataList;
  }


}



