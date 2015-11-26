package com.datatorrent.module.io.fs;

import java.util.List;

import com.google.common.collect.Lists;
import com.datatorrent.lib.io.fs.FileSplitterInput;
import com.datatorrent.lib.io.input.ModuleFileSplitter.ModuleFileMetaData;
import com.datatorrent.module.io.fs.TrackerEvent.TrackerEventType;

/**
 * <p>IngestionFileSplitter class.</p>
 *
 * @since 1.0.0
 */
public class IngestionFileMetaData extends FileSplitterInput.FileMetadata implements OutputFileMetaData
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
    setRelativePath(getRelativePath());
    setFileName(getFileName());
    setNumberOfBlocks(getNumberOfBlocks());
    setDataOffset(getDataOffset());
    setFileLength(getFileLength());
    setDiscoverTime(getDiscoverTime());
    setBlockIds(getBlockIds());
    setDirectory(isDirectory());
    
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



