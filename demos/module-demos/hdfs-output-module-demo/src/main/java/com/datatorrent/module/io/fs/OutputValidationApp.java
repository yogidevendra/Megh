package com.datatorrent.module.io.fs;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.stream.DevNull;

@ApplicationAnnotation(name = "OutputValidationApp")
public class OutputValidationApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    OutputValidator outputValidator = dag.addOperator("OutputValidator", new OutputValidator());
    DevNull devnull = dag.addOperator("DevNull", new DevNull());

    dag.addStream("Dummy", outputValidator.output, devnull.data);
  }
}
