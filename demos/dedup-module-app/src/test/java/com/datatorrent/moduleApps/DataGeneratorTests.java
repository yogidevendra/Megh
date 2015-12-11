package com.datatorrent.moduleApps;

import org.junit.Test;

import com.datatorrent.modules.utils.BaseDataGenerator;
import com.datatorrent.modules.utils.OrderedDataGenerator;
import com.datatorrent.modules.utils.SystemTimeDataGenerator;
import com.datatorrent.modules.utils.TimeDataGenerator;

public class DataGeneratorTests
{

  @Test
  public void testNoExpiry()
  {
    BaseDataGenerator dg = new BaseDataGenerator(100);
    for (int i = 0; i < 100; i++) {
      System.out.println(dg.generateNextTuple());
    }
  }

  @Test
  public void testOrdered()
  {
    OrderedDataGenerator dg = new OrderedDataGenerator(10000, 100);
    for (int i = 0; i < 10000; i++) {
      System.out.println(dg.generateNextTuple());
    }
  }

  @Test
  public void testTupleTime()
  {
    TimeDataGenerator dg = new TimeDataGenerator(10000, 60 * 1000);
    for (int i = 0; i < 10000; i++) {
      System.out.println(dg.generateNextTuple());
    }
  }

  @Test
  public void testSystemTime()
  {
    SystemTimeDataGenerator dg = new SystemTimeDataGenerator(100000, 2 * 1000); // expiry period 2 secs
    for (int i = 0; i < 100000; i++) {
      System.out.println(dg.generateNextTuple());
    }
  }
}
