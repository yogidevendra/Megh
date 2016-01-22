package com.datatorrent.module.io.fs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.ReaderContext;
import com.datatorrent.operator.HDFSBlockReader;

public class HDFSRecordReader extends HDFSBlockReader
{
  public static enum RECORD_READER_MODE
  {
    DELIMITED_RECORD, FIXED_WIDTH_RECORD;
  }

  RECORD_READER_MODE mode;
  int recordLength;
  
  public final transient DefaultOutputPort<byte[]> records = new DefaultOutputPort<byte[]>();

  public HDFSRecordReader()
  {
    mode = RECORD_READER_MODE.DELIMITED_RECORD;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (mode == RECORD_READER_MODE.FIXED_WIDTH_RECORD) {
      ReaderContext.FixedBytesReaderContext<FSDataInputStream> fixedBytesReaderContext = new ReaderContext.FixedBytesReaderContext<FSDataInputStream>();
      fixedBytesReaderContext.setLength(recordLength);
      readerContext = fixedBytesReaderContext;
    } else {
      readerContext = new ReaderContext.ReadAheadLineReaderContext<FSDataInputStream>();
    }

  }
  
  /**
   * Override this if you want to change how much of the block is read.
   *
   * @param blockMetadata block
   * @throws IOException
   */
  protected void readBlock(BlockMetadata blockMetadata) throws IOException
  {
    readerContext.initialize(stream, blockMetadata, consecutiveBlock);
    ReaderContext.Entity entity;
    while ((entity = readerContext.next()) != null) {

      counters.getCounter(ReaderCounterKeys.BYTES).add(entity.getUsedBytes());

      byte[] record = entity.getRecord();

      //If the record is partial then ignore the record.
      if (record != null) {
        counters.getCounter(ReaderCounterKeys.RECORDS).increment();
        records.emit(record);
      }
    }
  }

  public void setMode(RECORD_READER_MODE mode)
  {
    this.mode = mode;
  }

  public RECORD_READER_MODE getMode()
  {
    return mode;
  }

  public void setRecordLength(int recordLength)
  {
    this.recordLength = recordLength;
  }

  public int getRecordLength()
  {
    return recordLength;
  }
}
