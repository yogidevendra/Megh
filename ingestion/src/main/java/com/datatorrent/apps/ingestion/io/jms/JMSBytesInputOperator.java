/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io.jms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Enumeration;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.IdempotentStorageManager.FSIdempotentStorageManager;
import com.datatorrent.lib.io.jms.AbstractJMSInputOperator;
import com.datatorrent.apps.ingestion.io.BandwidthLimitingOperator;
import com.datatorrent.apps.ingestion.lib.BandwidthManager;


/**
 * JMS input operator for which outputs byte[]
 *
 * @since 1.0.0
 */
public class JMSBytesInputOperator extends AbstractJMSInputOperator<byte[]> implements BandwidthLimitingOperator
{

  /**
   * Separator for key value pair in Map message
   */
  private String keyValueSeparator = ":";
  private byte [] buffer = new byte[1000];
  private String recoveryDir = "idempotency";
  private BandwidthManager bandwidthManager;
  /**
   * Separator for (key:value) entry in Map message
   */
  private String entrySeparator = ",";
  
  
  @AutoMetric
  private long inputMessagesPerSec;
  
  @AutoMetric
  private long inputBytesPerSec;
  
  private long messageCount;
  private long byteCount;
  private double windowTimeSec; 
  
  /**
   * 
   */
  public JMSBytesInputOperator()
  {
    super();
    ((FSIdempotentStorageManager) idempotentStorageManager).setRecoveryPath(recoveryDir);
    bandwidthManager = new BandwidthManager();
  }
  
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    inputMessagesPerSec = 0;
    inputBytesPerSec = 0;
    messageCount = 0;
    byteCount = 0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    inputBytesPerSec = (long) (byteCount / windowTimeSec);
    inputMessagesPerSec = (long) (messageCount / windowTimeSec);
  }

  /* (non-Javadoc)
   * @see com.datatorrent.lib.io.jms.AbstractJMSInputOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext context)
  {
    ((FSIdempotentStorageManager) idempotentStorageManager).setRecoveryPath(recoveryDir);
    super.setup(context);
    bandwidthManager.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }
  
  /* (non-Javadoc)
   * @see com.datatorrent.lib.io.jms.AbstractJMSInputOperator#convert(javax.jms.Message)
   */
  @Override
  protected byte[] convert(Message message) throws JMSException
  {
    if(message instanceof TextMessage){
      return ((TextMessage)message).getText().getBytes();
    }
    else if(message instanceof StreamMessage){
      try {
        return readStreamMessage((StreamMessage) message);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    else if(message instanceof BytesMessage){
      BytesMessage bytesMessage = (BytesMessage) message;
      byte[] bytes = new byte[(int)bytesMessage.getBodyLength()];
      bytesMessage.readBytes(bytes);
      return bytes;
    }
    else if(message instanceof MapMessage){
      return readMapMessage((MapMessage) message);
    }
    else if(message instanceof ObjectMessage){
      try {
        return readObjectMessage((ObjectMessage) message);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    throw new IllegalArgumentException("Message Type " + message.getJMSType() + " is not supported.");
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      return;
    }

    while (emitCount < bufferSize) {
      if (!bandwidthManager.canConsumeBandwidth()) {
        return;
      }
      Message msg = holdingBuffer.poll();
      if(msg == null) {
        return;
      }
      processMessage(msg);
      emitCount++;
      lastMsg = msg;
    }
  }
  

  /* (non-Javadoc)
   * @see com.datatorrent.lib.io.jms.AbstractJMSInputOperator#emit(java.lang.Object)
   */
  @Override
  protected void emit(byte[] payload)
  {
    messageCount++;
    byteCount += payload.length;
    output.emit(payload);
    bandwidthManager.consumeBandwidth(payload.length);
  }
  
  /**
   * Extract a byte[] from the given {@link StreamMessage}.
   * @param message
   * @return
   * @throws JMSException 
   * @throws IOException 
   */
  private byte[] readStreamMessage(StreamMessage message) throws JMSException, IOException
  {
    
      ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();
      try{  int bytesRead = 0;
      do{
        bytesRead = message.readBytes(buffer);
        bytesOutStream.write(buffer, 0 , bytesRead);
      }while(bytesRead == buffer.length);
      return bytesOutStream.toByteArray();
    }
    finally{
      bytesOutStream.close(); 
    }
  }
  
  /**
   * Extract a byte[] from the given {@link MapMessage}.
   * byte[] representation of String generated by iterating over key: value pairs.
   * Key and value are separated using <code>keyValueSeparator</code>.
   * (Key:Value) entries are separated using <code>entrySeparator</code>.
   * 
   * @param message the message to convert
   * @return the resulting byte[]
   * @throws JMSException if thrown by JMS methods
   */
  protected byte[] readMapMessage(MapMessage message) throws JMSException
  {
    StringBuffer sb = new StringBuffer();
    Enumeration<?> en = message.getMapNames();
    sb.append("{");
    while(en.hasMoreElements()){
      String key = (String) en.nextElement();
      sb.append(key);
      sb.append(keyValueSeparator);
      sb.append(message.getObject(key));
      sb.append(entrySeparator);
      
    }
    sb.deleteCharAt(sb.length()-1);
    sb.append("}");
    return sb.toString().getBytes();
  }

  /**
   * Extract a byte[] from the given {@link ObjectMessage}.
   *
   * @param message the message to convert
   * @return the resulting byte[]
   * @throws JMSException if thrown by JMS methods
   * @throws IOException 
   */
  protected byte[] readObjectMessage(ObjectMessage message) throws JMSException, IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream( baos );
    try{
      oos.writeObject(message.getObject());
      return baos.toByteArray();
    }
    finally{
      oos.close();
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    bandwidthManager.teardown();
  }

  /**
   * @return the keyValueSeparator
   */
  public String getKeyValueSeparator()
  {
    return keyValueSeparator;
  }
  
  /**
   * @param keyValueSeparator the keyValueSeparator to set
   */
  public void setKeyValueSeparator(String keyValueSeparator)
  {
    this.keyValueSeparator = keyValueSeparator;
  }
  
  /**
   * @return the entrySepertor
   */
  public String getEntrySeparator()
  {
    return entrySeparator;
  }
  
  /**
   * @param entrySepertor the entrySepertor to set
   */
  public void setEntrySeparator(String entrySeparator)
  {
    this.entrySeparator = entrySeparator;
  }
  
  /**
   * @return the recoveryDir
   */
  public String getRecoveryDir()
  {
    return recoveryDir;
  }
  
  /**
   * @param recoveryDir the recoveryDir to set
   */
  public void setRecoveryDir(String recoveryDir)
  {
    this.recoveryDir = recoveryDir;
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

}
