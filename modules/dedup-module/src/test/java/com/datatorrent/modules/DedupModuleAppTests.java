package com.datatorrent.modules;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.LocalMode;

/**
 * Unit test for simple App.
 */
public class DedupModuleAppTests
{
  FileSystem fs = null;
  String outputPath = "target";

  @Before
  public void setup() throws IllegalArgumentException, IOException
  {
    fs = FileSystem.newInstance(new Path(outputPath).toUri(), new Configuration());
  }

  public void validate(boolean isExpiry) throws IllegalArgumentException, IOException
  {
    //Check if files exist
    Assert.assertTrue("Output dir does not exist", fs.exists(new Path(outputPath)));
    Assert.assertTrue("Uniques not generated", fs.exists(new Path(outputPath+"/unique")));
    Assert.assertTrue("Duplicates not generated", fs.exists(new Path(outputPath+"/duplicate")));
    if(isExpiry) {
      Assert.assertTrue("Expired not generated", fs.exists(new Path(outputPath+"/expired")));
    }

    //Check if result is correct
    Assert.assertFalse("Uniques has DUPLICATE", checkKeywordInFile(outputPath+"/unique", "DUPLICATE"));
    Assert.assertFalse("Duplicates has UNIQUE", checkKeywordInFile(outputPath+"/duplicate", "UNIQUE"));
    if(isExpiry) {
      Assert.assertFalse("Uniques has EXPIRED", checkKeywordInFile(outputPath+"/unique", "EXPIRED"));
      Assert.assertFalse("Duplicates has EXPIRED", checkKeywordInFile(outputPath+"/duplicate", "EXPIRED"));
      Assert.assertFalse("Expired has UNIQUE", checkKeywordInFile(outputPath+"/expired", "UNIQUE"));
      Assert.assertFalse("Expired has DUPLICATE", checkKeywordInFile(outputPath+"/expired", "DUPLICATE"));
    }
  }

  public boolean checkKeywordInFile(String path, String keyword) throws IOException{
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(path));
      String s = "";
      while((s = br.readLine()) != null){
        if(s.toUpperCase().contains(keyword.toUpperCase())){
          return true;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    finally{
      br.close();
    }
    return false;
  }

  // Test no expiry
  @Test
  public void testApplicationNoExpiry() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/properties_noexpiry.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(8000); // runs for 60 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
    validate(false);
  }

  // Test ordered
  @Test
  public void testApplicationOrdered() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/properties_ordered.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 60 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
    validate(true);
  }

  // Test tupletime
  @Test
  public void testApplicationTupletime() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/properties_tupletime.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(20000); // runs for 300 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
    validate(true);
  }

  // Test systemtime
  @Test
  public void testApplicationSystemtime() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/properties_systemtime.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(20000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
    validate(true);
  }

  // Test categorical
  @Test
  public void testApplicationCategorical() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/properties_categorical.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
    validate(true);
  }
}
