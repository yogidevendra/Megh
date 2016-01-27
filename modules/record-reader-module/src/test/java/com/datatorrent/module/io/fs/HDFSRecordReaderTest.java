/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.module.io.fs;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

public class HDFSRecordReaderTest
{
  private String inputDir;
  static String outputDir;
  private static final String FILE_1 = "file1.txt";
  private static final String FILE_2 = "file2.txt";
  private static final String FILE_1_DATA = "1234\n567890\nabcde\nfgh\ni\njklmop";
  private static final String FILE_2_DATA = "qr\nstuvw\nxyz\n";

  public static class TestMeta extends TestWatcher
  {
    public String baseDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Before
  public void setup() throws Exception
  {
    inputDir = testMeta.baseDirectory + File.separator + "input";

    FileUtils.writeStringToFile(new File(inputDir + File.separator + FILE_1), FILE_1_DATA);
    FileUtils.writeStringToFile(new File(inputDir + File.separator + FILE_2), FILE_2_DATA);
  }

  @Test
  public void testDelimitedRecords() throws Exception
  {
    
    DelimitedApplication app = new DelimitedApplication();
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.HDFSRecordReaderModule.prop.files", inputDir);
    conf.set("dt.operator.HDFSRecordReaderModule.prop.mode", "DELIMITED_RECORD");
    conf.set("dt.operator.HDFSRecordReaderModule.prop.scanIntervalMillis", "10000");

    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();
    LOG.debug("Waiting for app to finish");
    Thread.sleep(1000*1);
    lc.shutdown();
  }

  public static class DelimitedValidator extends BaseOperator
  {
    Set<String> records = new HashSet<String>();

    public final transient DefaultInputPort<byte[]> data = new DefaultInputPort<byte[]>()
    {

      @Override
      public void process(byte[] tuple)
      {
        String record = new String(tuple);
        records.add(record);
      }
    };
    
    public void teardown() {
      Set<String> expectedRecords = new HashSet<String>(Arrays.asList(FILE_1_DATA.split("\n")));
      expectedRecords.addAll(Arrays.asList(FILE_2_DATA.split("\n")));
      
      Assert.assertEquals(expectedRecords, records);
    };
  }

  private static class DelimitedApplication implements StreamingApplication
  {
    
    public void populateDAG(DAG dag, Configuration conf)
    {
      HDFSRecordReaderModule recordReader = dag.addModule("HDFSRecordReaderModule", HDFSRecordReaderModule.class);
      DelimitedValidator validator = dag.addOperator("Validator", new DelimitedValidator());
      dag.addStream("records", recordReader.records, validator.data);
    }
    
  }
  
  @Test
  public void testFixedWidthRecords() throws Exception
  {
    
    FixedWidthApplication app = new FixedWidthApplication();
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.HDFSRecordReaderModule.prop.files", inputDir);
    conf.set("dt.operator.HDFSRecordReaderModule.prop.mode", "FIXED_WIDTH_RECORD");
    conf.set("dt.operator.HDFSRecordReaderModule.prop.recordLength", "8");
    conf.set("dt.operator.HDFSRecordReaderModule.prop.scanIntervalMillis", "10000");

    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();
    LOG.debug("Waiting for app to finish");
    Thread.sleep(1000*1);
    lc.shutdown();
  }

  public static class FixedWidthValidator extends BaseOperator
  {
    Set<String> records = new HashSet<String>();

    public final transient DefaultInputPort<byte[]> data = new DefaultInputPort<byte[]>()
    {

      @Override
      public void process(byte[] tuple)
      {
        String record = new String(tuple);
        records.add(record);
      }
    };
    
    public void teardown() {
      String [] expected = {"1234\n567","890\nabcd","e\nfgh\ni\n","jklmop","qr\nstuvw","\nxyz\n"};
      
      Set<String> expectedRecords = new HashSet<String>(Arrays.asList(expected));
      
      Assert.assertEquals(expectedRecords, records);
    };
  }

  private static class FixedWidthApplication implements StreamingApplication
  {
    
    public void populateDAG(DAG dag, Configuration conf)
    {
      HDFSRecordReaderModule recordReader = dag.addModule("HDFSRecordReaderModule", HDFSRecordReaderModule.class);
      FixedWidthValidator validator = dag.addOperator("Validator", new FixedWidthValidator());
      dag.addStream("records", recordReader.records, validator.data);
    }
    
  }
  

  private static Logger LOG = LoggerFactory.getLogger(HDFSRecordReaderTest.class);

}
