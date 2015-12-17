package com.datatorrent.module.io.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * This class is responsible for writing tuples to HDFS. All tuples are written
 * to the same file. Rolling over to the next file based on file size is
 * supported.
 *
 * @param <T>
 */
class HDFSFileOutputOperator<T> extends AbstractFileOutputOperator<T>
{

  /**
   * Name of the file to write the output. Directory for the output file should
   * be specified in the filePath
   */
  @NotNull
  private String fileName;

  /**
   * Separator between the tuples
   */
  @NotNull
  private String tupleSeparator;

  private byte[] tupleSeparatorBytes;

  @AutoMetric
  private long bytesPerSec;

  private long byteCount;
  private double windowTimeSec;

  /**
   * File Name format for output files
   */
  private String outputFileNameFormat = "%s.%d";

  private String tupleFileName;

  private int operatorId;

  private static final long STREAM_EXPIRY_ACCESS_MILL = 24 * 60 * 60 * 1000L;
  private static final int ROTATION_WINDOWS = 2 * 60 * 60 * 20;

  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileOutputOperator.class);

  public HDFSFileOutputOperator()
  {

    setExpireStreamAfterAccessMillis(STREAM_EXPIRY_ACCESS_MILL);
    setMaxOpenFiles(1000);
    // Rotation window count = 20 hrs which is < expirestreamafteraccessmillis
    setRotationWindows(ROTATION_WINDOWS);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getFileName(T tuple)
  {
    return tupleFileName;
  }

  /**
   * {@inheritDoc}
   * 
   * @return byte[] representation of the given tuple. if input tuple is of type
   *         byte[] then it is returned as it is. for any other type toString()
   *         representation is used to generate byte[].
   */
  @Override
  protected byte[] getBytesForTuple(T tuple)
  {
    byte[] tupleData;
    if (tuple instanceof byte[]) {
      tupleData = (byte[])tuple;
    } else {
      tupleData = tuple.toString().getBytes();
    }

    ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();

    try {
      bytesOutStream.write(tupleData);
      bytesOutStream.write(tupleSeparatorBytes);
      return bytesOutStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        bytesOutStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void writeTupleDataObject(ByteArrayOutputStream bytesOutStream, T tuple) throws IOException
  {
    ObjectOutputStream objectOutputStream;
    objectOutputStream = new ObjectOutputStream(bytesOutStream);
    objectOutputStream.writeObject(tuple);
    objectOutputStream.close();
  }

  private final long getHDFSBlockSize()
  {
    //Initialize the maxLength to HDFS block size if it is not already set 
    try {
      FileSystem outputFS = FileSystem.newInstance(URI.create(filePath), new Configuration());
      long blockSize = outputFS.getDefaultBlockSize(new Path(filePath));
      outputFS.close();

      return blockSize;
    } catch (IOException e) {
      throw new RuntimeException("Unable to get FileSystem instance for computing .", e);
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    //Make sure each partition writes to different file by appending operatorId 
    operatorId = context.getId();
    tupleFileName = String.format(outputFileNameFormat, fileName, operatorId);
    LOG.debug("maxLength :{}", maxLength);
    if (maxLength == Long.MAX_VALUE) {
      long blockSize = getHDFSBlockSize();
      maxLength = blockSize;
      LOG.debug("setMaxLength blockSize:{}", blockSize);
    }

    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)
        * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    bytesPerSec = 0;
    byteCount = 0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    bytesPerSec = (long)(byteCount / windowTimeSec);
  }

  /**
   * @return File name for writing output. All tuples are written to the same
   *         file.
   * 
   */
  public String getFileName()
  {
    return fileName;
  }

  /**
   * @param fileName
   *          File name for writing output. All tuples are written to the same
   *          file.
   */
  public void setFileName(String fileName)
  {
    this.fileName = fileName;
    tupleFileName = String.format(outputFileNameFormat, fileName, operatorId);
  }

  /**
   * @return Separator between the tuples
   */
  public String getTupleSeparator()
  {
    return tupleSeparator;
  }

  /**
   * @param separator
   *          Separator between the tuples
   */
  public void setTupleSeparator(String separator)
  {
    this.tupleSeparator = separator;
    this.tupleSeparatorBytes = separator.getBytes();
  }

}
