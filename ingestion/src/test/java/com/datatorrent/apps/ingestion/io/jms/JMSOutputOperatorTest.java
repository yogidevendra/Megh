package com.datatorrent.apps.ingestion.io.jms;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.apps.ingestion.io.jms.JMSOutputOperator;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.jms.FSPsuedoTransactionableStore;
import com.datatorrent.lib.io.jms.JMSTestBase;
import com.datatorrent.lib.util.ActiveMQMultiTypeMessageListener;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import javax.jms.JMSException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to verify JMS output operator adapter.
 */
public class JMSOutputOperatorTest extends JMSTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(JMSOutputOperatorTest.class);
  public static int tupleCount = 0;
  public static final transient int maxTuple = 20;

  public static final String CLIENT_ID = "Client1";
  public static final String APP_ID = "appId";
  public static final int OPERATOR_ID = 1;
  public static JMSOutputOperator outputOperator;
  public static OperatorContextTestHelper.TestIdOperatorContext testOperatorContext;
  public static OperatorContextTestHelper.TestIdOperatorContext testOperatorContextAMO;
  public static final int HALF_BATCH_SIZE = 5;
  public static final int BATCH_SIZE = HALF_BATCH_SIZE * 2;
  public static final Random random = new Random();

  public static class TestMeta extends TestWatcher
  {
    @Override
    protected void starting(org.junit.runner.Description description)
    {
      logger.debug("Starting test {}", description.getMethodName());
      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_ID, APP_ID);
      testOperatorContext = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes);

      attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_MOST_ONCE);
      testOperatorContextAMO = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes);

      try {
        FileUtils.deleteDirectory(new File(FSPsuedoTransactionableStore.DEFAULT_RECOVERY_DIRECTORY));
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    protected void finished(org.junit.runner.Description description)
    {
      try {
        FileUtils.deleteDirectory(new File(FSPsuedoTransactionableStore.DEFAULT_RECOVERY_DIRECTORY));
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  /**
   * This is a helper method to create the output operator. Note this cannot be
   * put in test watcher because because JMS connection issues occur when this code
   * is run from a test watcher.
   */
  private void createOperator(boolean topic, Context.OperatorContext context)
  {
    createOperator(topic, context, 0);
  }

  private void createOperator(boolean topic, Context.OperatorContext context, @SuppressWarnings("unused") int maxMessages)
  {
    outputOperator = new JMSOutputOperator();
    outputOperator.getConnectionFactoryProperties().put("userName", "");
    outputOperator.getConnectionFactoryProperties().put("password", "");
    outputOperator.getConnectionFactoryProperties().put("brokerURL", "tcp://localhost:61617");
    outputOperator.setAckMode("CLIENT_ACKNOWLEDGE");
    outputOperator.setClientId(CLIENT_ID);
    outputOperator.setSubject("TEST.FOO");

    outputOperator.setMessageSize(255);
    outputOperator.setBatch(BATCH_SIZE);
    outputOperator.setTopic(topic);
    outputOperator.setDurable(false);
    outputOperator.setVerbose(true);
    outputOperator.setup(context);
  }

  /**
   * Test AbstractJMSOutputOperator (i.e. an output adapter for JMS, aka producer).
   * This module sends data into an JMS message bus.
   *
   * [Generate tuple] ==> [send tuple through JMS output adapter(i.e. producer) into JMS message bus]
   * ==> [receive data in outside JMS listener]
   *
   * @throws Exception
   */
  //@Ignore
  @Test
  public void testJMSOutputOperator1() throws Exception
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setupConnection();
    listener.run();

    createOperator(false, testOperatorContext, 15);

    outputOperator.beginWindow(1);

    int i = 0;
    while (i < maxTuple) {
      logger.debug("Emitting tuple {}", i);
      String tuple = "testString " + (++i);
      outputOperator.inputPort.process(tuple);
      tupleCount++;
    }

    outputOperator.endWindow();
    outputOperator.teardown();

    Thread.sleep(500);

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(1));

    listener.closeConnection();
  }

  /**
   * This test is same as prior one except maxMessage and topic setting is different.
   *
   * @throws Exception
   */
  ////@Ignore
  @Test
  public void testJMSOutputOperator2() throws Exception
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(true);
    listener.setupConnection();
    listener.run();

    createOperator(true, testOperatorContext, 15);

    outputOperator.beginWindow(1);

    // produce data and process

    int i = 0;
    while (i < maxTuple) {
      String tuple = "testString " + (++i);
      outputOperator.inputPort.process(tuple);
      tupleCount++;
    }

    outputOperator.endWindow();
    outputOperator.teardown();

    Thread.sleep(500);

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(1));

    listener.closeConnection();
  }

  //@Ignore
  @Test
  public void testBatch()
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    try {
      listener.setupConnection();
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    listener.run();

    createOperator(false, testOperatorContext);

    outputOperator.beginWindow(0);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(1);

    for(int batchCounter = 0;
        batchCounter < HALF_BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should not be written",
        BATCH_SIZE + HALF_BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(2);

    for(int batchCounter = 0;
        batchCounter < HALF_BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should not be written",
        2 * BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.teardown();

    listener.closeConnection();
  }

  //@Ignore
  @Test
  public void testAtLeastOnceFullBatch()
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    try {
      listener.setupConnection();
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    listener.run();

    createOperator(false, testOperatorContext);

    outputOperator.beginWindow(0);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(1);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.teardown();

    ////

    outputOperator.setup(testOperatorContext);

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(0);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(1);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        2 * BATCH_SIZE,
        listener.receivedData.size());

    listener.closeConnection();
  }

  //@Ignore
  @Test
  public void testAtLeastOnceHalfBatch()
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    try {
      listener.setupConnection();
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    listener.run();

    createOperator(false, testOperatorContext);

    outputOperator.beginWindow(0);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(1);

    for(int batchCounter = 0;
        batchCounter < HALF_BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.teardown();

    ////

    outputOperator.setup(testOperatorContext);

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(0);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(1);

    for(int batchCounter = 0;
        batchCounter < HALF_BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE + HALF_BATCH_SIZE,
        listener.receivedData.size());

    listener.closeConnection();
  }

  //@Ignore
  @Test
  public void testAtMostOnceFullBatch()
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    try {
      listener.setupConnection();
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    listener.run();

    createOperator(false, testOperatorContextAMO);

    outputOperator.beginWindow(0);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(1);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.teardown();

    ////

    outputOperator.setup(testOperatorContext);

    outputOperator.beginWindow(2);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        2 * BATCH_SIZE,
        listener.receivedData.size());

    listener.closeConnection();
  }

  //@Ignore
  @Test
  public void testAtMostOnceHalfBatch()
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    try {
      listener.setupConnection();
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    listener.run();

    createOperator(false, testOperatorContextAMO);

    outputOperator.beginWindow(0);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(1);

    for(int batchCounter = 0;
        batchCounter < HALF_BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.teardown();

    ////

    outputOperator.setup(testOperatorContext);

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        BATCH_SIZE,
        listener.receivedData.size());

    outputOperator.beginWindow(2);

    for(int batchCounter = 0;
        batchCounter < BATCH_SIZE;
        batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written",
        2 * BATCH_SIZE,
        listener.receivedData.size());

    listener.closeConnection();
  }
}

