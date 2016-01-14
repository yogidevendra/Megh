/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.module.io.fs;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;

import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Splitter;

/**
 * This class is for performance comparison for Java String.split() Vs
 * Gauva.Splitter Sample output which shows String.split better than
 * Gauva.splitter
 */

/*
Sample benchmark resultson 

$java -version
java version "1.7.0_80"
Java(TM) SE Runtime Environment (build 1.7.0_80-b15)
Java HotSpot(TM) 64-Bit Server VM (build 24.80-b11, mixed mode)

And using guava-11.0.2.jar
 
runBenchMark - Using Delimiter "\n"
testStringSplit - Time taken 313342353 us, length sum = 134190000
testGauvaSplit -  Time taken 458233242 us, length sum =134190000
runBenchMark -  String split better than  split by 1.4624044199987225X

runBenchMark - Using Delimiter ###123### 
testStringSplit - Time taken 98182805 us, length sum = 134001795
testGauvaSplit -  Time taken 329800813 us, length sum =134001795
runBenchMark -  String split better than  split by 3.359048593081039X

runBenchMark - Using Delimiter ###123456789012345678901234567890#### 
testStringSplit - Time taken 45178713 us, length sum = 133256646
testGauvaSplit -  Time taken 480220819 us, length sum =133256646
runBenchMark -  String split better than  split by 10.629360314004519X

*/
public class SplitterBenchmarkTest
{
  public static class TestMeta extends TestWatcher
  {

    private final int messageSize = 5000; //%K bytes per message
    private final int blockSize = 128 * 1024 * 1024; //128MB

    private final String delimiterString;
    private final byte[] delimiterBytes;

    private final int noOfMessages;
    ByteArrayOutputStream baos = new ByteArrayOutputStream(blockSize);
    String input;

    public TestMeta(String delimiterString)
    {
      this.delimiterString = delimiterString;
      delimiterBytes = delimiterString.getBytes();
      noOfMessages = blockSize / (messageSize + delimiterString.length());
      init();
    }

    private void init()
    {
      for (int i = 0; i < noOfMessages; i++) {
        String data = RandomStringUtils.random(messageSize, true, true);
        baos.write(data.getBytes(), 0, data.length());
        baos.write(delimiterBytes, 0, 1);
      }
      input = new String(baos.toByteArray());
    }
  }

  @Test
  public void runBenchMark()
  {
    String[] delimiters = { "\n", "###123###", "###123456789012345678901234567890####" };

    for (String delimiter : delimiters) {
      LOG.info("Using Delimiter {} ", delimiter);
      TestMeta testMeta = new TestMeta(delimiter);
      
      long r1 = testStringSplit(testMeta);
      long r2 = testGauvaSplit(testMeta);
      long r3 = testApacheStringUtilsSplit(testMeta);
      
      if (r1 < r2) {
        LOG.info(" String split better than  split by {}X", r2 * 1.0 / r1);
      } else {
        LOG.info(" Gauva split better than Gauva String by {}X", r1 * 1.0 / r2);
      }
    }
  }

  public long testStringSplit(TestMeta testMeta)
  {
    long begin = System.nanoTime();
    String[] tokens = testMeta.input.split(testMeta.delimiterString);
    int total = 0;
    for (String token : tokens) {
      total += token.length();
    }

    long end = System.nanoTime();
    LOG.info("Time taken {} us, length sum = {}", end - begin, total);
    return end - begin;
  }
  
  public long testApacheStringUtilsSplit(TestMeta testMeta)
  {
    long begin = System.nanoTime();
    
    String[] tokens = StringUtils.split(testMeta.input, testMeta.delimiterString);
    int total = 0;
    for (String token : tokens) {
      total += token.length();
    }

    long end = System.nanoTime();
    LOG.info("Time taken {} us, length sum = {}", end - begin, total);
    return end - begin;
  }

  public long testGauvaSplit(TestMeta testMeta)
  {
    Splitter split = Splitter.on(testMeta.delimiterString);
    long begin = System.nanoTime();
    Iterator<String> it = split.split(testMeta.input).iterator();
    int total = 0;
    while (it.hasNext()) {
      String token = it.next();
      total += token.length();
    }

    long end = System.nanoTime();
    LOG.info(" Time taken {} us, length sum ={}", end - begin, total);

    return end - begin;
  }

  private static Logger LOG = LoggerFactory.getLogger(SplitterBenchmarkTest.class);

}
