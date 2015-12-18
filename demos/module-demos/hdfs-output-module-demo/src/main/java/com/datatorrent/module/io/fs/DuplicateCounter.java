package com.datatorrent.module.io.fs;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class DuplicateCounter extends BaseOperator
{
  private static Logger LOG = LoggerFactory.getLogger(DuplicateCounter.class);
  
  private static final String STATUS_FILE = "DEDUP.success";
  Path statusPath;

  int tuple_count = 0;
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    statusPath = new Path(context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + STATUS_FILE);
  }

  public final transient DefaultInputPort<Object> data = new DefaultInputPort<Object>()
  {
    public void process(Object tuple)
    {
      ++tuple_count;
    }
  };

  @Override
  public void teardown()
  {
    
    if (tuple_count == 0) {
      try {
        FileSystem fs = FileSystem.newInstance(statusPath.toUri(), new Configuration());
        fs.create(statusPath, true);
      } catch (IOException e) {
        throw new RuntimeException();
      }
    } else {
      LOG.info("VALIDATION FAILED: DUPLICATES FOUND");
    }
    super.teardown();
  }
}
