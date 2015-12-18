package com.datatorrent.module.io.fs;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.bucket.BasicBucketManagerPOJOImpl;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.dedup.DeduperPOJOImpl;
import com.datatorrent.lib.stream.DevNullCounter;

@ApplicationAnnotation(name = "OutputValidationApp")
public class OutputValidationApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    OutputValidator outputValidator = dag.addOperator("OutputValidator", new OutputValidator());
    DeduperPOJOImpl deduperPOJOImpl = dag.addOperator("DeduperPOJOImpl", new DeduperPOJOImpl());
    DuplicateCounter duplicateCounter = dag.addOperator("DuplicateCounter", new DuplicateCounter());
    DevNullCounter<Object> uniqueCounter = dag.addOperator("UniqueCounter", new DevNullCounter<Object>());
    
    dag.addStream("ValidatorToDedup", outputValidator.messages, deduperPOJOImpl.input)
      .setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Duplicates", deduperPOJOImpl.duplicates, duplicateCounter.data);
    dag.addStream("Uniques", deduperPOJOImpl.output, uniqueCounter.data);
  
    HdfsBucketStore store = new HdfsBucketStore();
    BasicBucketManagerPOJOImpl basicBucketManagerPOJOImpl = new BasicBucketManagerPOJOImpl();
    basicBucketManagerPOJOImpl.setKeyExpression("id");
    basicBucketManagerPOJOImpl.setNoOfBuckets(10000);
    basicBucketManagerPOJOImpl.setBucketsInMemory(10000);
    basicBucketManagerPOJOImpl.setMaxNoOfBucketsInMemory(10000);
    basicBucketManagerPOJOImpl.setBucketStore(store);
    deduperPOJOImpl.setBucketManager(basicBucketManagerPOJOImpl);
    
    
  }
}
