package com.datatorrent.module.io.fs;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.input.BlockReader;
import com.datatorrent.lib.io.input.ModuleFileSplitter;
import com.datatorrent.module.FSInputModule;
import com.datatorrent.module.io.fs.HDFSRecordReader.RECORD_READER_MODE;
import com.datatorrent.operator.HDFSFileSplitter;

public class HDFSRecordReaderModule extends FSInputModule
{
  
  public final transient ProxyOutputPort<byte[]> records = new ProxyOutputPort<byte[]>();
  
  String mode;
  
  int recordLength;
  
  
  
  @Override
  public ModuleFileSplitter getFileSplitter()
  {
    return new HDFSFileSplitter();
  }
  
  HDFSRecordReader recordReader;
  
  @Override
  public BlockReader getBlockReader()
  {
    recordReader = new HDFSRecordReader();
    RECORD_READER_MODE readerMode = RECORD_READER_MODE.valueOf(mode);
    recordReader.setMode(readerMode);
    recordReader.setRecordLength(recordLength);
    
    recordReader.setUri(getFiles());
    return recordReader;
  }
  
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    super.populateDAG(dag, configuration);
    records.set(recordReader.records);
  }
  
  public String getMode()
  {
    return mode;
  }
  
  public void setMode(String mode)
  {
    this.mode = mode;
  }
  
  public int getRecordLength()
  {
    return recordLength;
  }
  
  public void setRecordLength(int recordLength)
  {
    this.recordLength = recordLength;
  }
}
