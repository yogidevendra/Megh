/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.storage;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Gaurav Gupta  <gaurav@datatorrent.com>
 */
public class HDFSStoragePerformance
{

  @Test
  public void testPerformance()
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, ".");
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(false));
    HDFSStorage storage = new HDFSStorage();
    storage.configure(ctx);

    logger.debug(" start time {}",System.currentTimeMillis());
    byte[] b = new byte[1024];
    for (int i = 0; i < 1000000; i++) {
      storage.store(b);
    }
    storage.close();
    logger.debug(" end time {}",System.currentTimeMillis());
  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStoragePerformance.class);
}
