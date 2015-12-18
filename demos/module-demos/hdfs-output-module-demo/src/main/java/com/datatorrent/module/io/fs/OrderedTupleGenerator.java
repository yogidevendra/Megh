package com.datatorrent.module.io.fs;

import java.io.Serializable;
import java.util.Random;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.RandomStringUtils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class OrderedTupleGenerator implements InputOperator
{
  //Properties
  private int messageSize = 1000; //1K bytes per message
  private int noOfMessages = 100 * 1000 * 1000; //100M messages

  private int maxTuplesPerWindow = 10 * 1000; ///10K messages per window
  private int messagesInCurrentWindow = 0;

  private long messageId = 0;
  Random random;

  private static final int M2 = 2 * 1000 * 1000;
  private static final int M1 = 1 * 1000 * 1000;

  private static final String randomData = RandomStringUtils.random(M2, true, true);
  public transient DefaultOutputPort<MessageWithCRCCheck> output = new DefaultOutputPort<MessageWithCRCCheck>();

  @Override
  public void beginWindow(long windowId)
  {
    messagesInCurrentWindow = 0;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    random = new Random();
  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void emitTuples()
  {

    if (messageId >= noOfMessages) {
      LOG.debug("Sending Shutdown trigger");
      throw new ShutdownException();
    }

    if (messagesInCurrentWindow >= maxTuplesPerWindow) {
      return;
    }

    int start = random.nextInt(M1);
    int end = start + messageSize;

    MessageWithCRCCheck message = new MessageWithCRCCheck(++messageId, randomData.substring(start, end));
    //    LOG.debug("Sending message id:{}", id);
    output.emit(message);
    ++messagesInCurrentWindow;
  }

  public int getMessageSize()
  {
    return messageSize;
  }

  public void setMessageSize(int messageSize)
  {
    this.messageSize = messageSize;
  }

  public int getNoOfMessages()
  {
    return noOfMessages;
  }

  public void setNoOfMessages(int noOfMessages)
  {
    this.noOfMessages = noOfMessages;
  }

  private static final Logger LOG = LoggerFactory.getLogger(OrderedTupleGenerator.class);

  public static class MessageWithCRCCheck implements Serializable
  {
    /**
     * 
     */
    private static final long serialVersionUID = 1190277461927445471L;
    public static final String separator = "|";
    long id;
    String data;
    long crc;

    private static final CRC32 CRC = new CRC32();

    private MessageWithCRCCheck()
    {
      //for Kryo
    }

    public MessageWithCRCCheck(long id, String data)
    {
      this.id = id;
      this.data = data;
      this.crc = computeCRC(data);
    }
    
    public static long computeCRC(String data)
    {
      CRC.reset();
      CRC.update(data.getBytes());
      return CRC.getValue();
    }
    
    public MessageWithCRCCheck(String tuple)
    {
      String[] tokens = tuple.split("\\|");
      long id = Long.parseLong(tokens[0]);
      String data = tokens[1];
      long crc = Long.parseLong(tokens[2]);
      
      this.id = id;
      this.data = data;
      this.crc = crc;
    }


    public long getId()
    {
      return id;
    }

    public String getData()
    {
      return data;
    }

    public long getCrc()
    {
      return crc;
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append(id);
      sb.append(separator);
      sb.append(data);
      sb.append(separator);
      sb.append(crc);
      return sb.toString();
    }
    
    

  }

}
