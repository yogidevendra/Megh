package com.datatorrent.module.io.fs;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.module.io.fs.OrderedTupleGenerator.MessageWithCRCCheck;

@ApplicationAnnotation(name = "HDFSOutputModuleDemo")
public class HDFSOutputModuleDemo implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    OrderedTupleGenerator orderedTupleGenerator = dag.addOperator("OrderedTupleGenerator", new OrderedTupleGenerator());
    HDFSOutputModule<MessageWithCRCCheck> outputModule = dag.addModule("HDFSOutput",
        new HDFSOutputModule<MessageWithCRCCheck>());
    dag.addStream("FileInput To HDFS Output", orderedTupleGenerator.output, outputModule.input);
  }

}
