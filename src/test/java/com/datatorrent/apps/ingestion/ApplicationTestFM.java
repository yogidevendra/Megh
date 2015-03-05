/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.LocalMode;
import com.google.common.collect.Sets;

/**
 * Test the DAG declaration in local mode.
 *
 * @author Yogi/Sandeep
 */
public class ApplicationTestFM
{
  public static class TestMeta extends TestWatcher
  {
    public String dataDirectory;
    public String baseDirectory;
    public String outputDirectory;
    public String recoveryDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
      this.recoveryDirectory = baseDirectory + "/recovery";
      this.outputDirectory = baseDirectory + "/output";
      this.dataDirectory = baseDirectory + "/data";
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testApplicationFM() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.FileSplitter.directory", testMeta.dataDirectory);
    conf.set("dt.operator.FileSplitter.idempotentStorageManager.recoveryPath", testMeta.recoveryDirectory);

    conf.set("dt.operator.BlockReader.directory", testMeta.dataDirectory);
    conf.set("dt.operator.FileMerger.prop.outputDir", testMeta.outputDirectory);
    conf.set("dt.application.Ingestion.attr.CHECKPOINT_WINDOW_COUNT","5");
    createFiles(testMeta.dataDirectory, 2,2);
    
    lma.prepareDAG(new ApplicationFM(), conf);
    lma.cloneDAG(); // check serialization
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    long now = System.currentTimeMillis();

    Path outDir = new Path(testMeta.outputDirectory);
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 60000) {
      Thread.sleep(500);
      LOG.debug("Waiting for {}", outDir);
    }
    Thread.sleep(1000);
    lc.shutdown();

    Assert.assertTrue("output dir does not exist", fs.exists(outDir));

    FileStatus[] statuses = fs.listStatus(outDir);
    Assert.assertTrue("file does not exist", statuses.length > 0 && fs.isFile(statuses[0].getPath()));

    FileUtils.deleteDirectory(new File("target/com.datatorrent.stram.StramLocalCluster"));
  }

  private void createFiles(String basePath, int numFiles, int numLines)
  {
    try {
      HashSet<String> allLines = Sets.newHashSet();
      for (int file = 0; file < numFiles; file++) {
        HashSet<String> lines = Sets.newHashSet();
        for (int line = 0; line < numLines; line++) {
          lines.add("f" + file + "l" + line);
        }
        allLines.addAll(lines);
        File created = new File(basePath, "/file" + file + ".txt");
        FileUtils.write(created, StringUtils.join(lines, '\n') + '\n');
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
}