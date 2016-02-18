/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.app;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kinesis.KinesisConsumer;


@ApplicationAnnotation(name = "KinesisToS3")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    AWSCredentials credentials = new BasicAWSCredentials("AKIAIY62OEFADWS65JZA", "CQeMB8vVgkOh6JnyY0hfq643RMTBhvRQFRtok5aa");
    
 // Create KinesisSinglePortStringInputOperator
    KinesisBytesInputOperator kinesisInput = dag.addOperator("KinesisMessageConsumer", KinesisBytesInputOperator.class);
    kinesisInput.setAccessKey(credentials.getAWSAccessKeyId());
    kinesisInput.setSecretKey(credentials.getAWSSecretKey());
    KinesisConsumer consumer = new KinesisConsumer();
    consumer.setStreamName("TestStream");
    consumer.setRecordsLimit(100);
    kinesisInput.setConsumer(consumer);

    //ConsoleOutputOperator collector = dag.addOperator("console", new ConsoleOutputOperator());
    S3BytesFileOutputOperator s3OutputOperator = dag.addOperator("FileWriter", new S3BytesFileOutputOperator());
    
    // Connect ports
    dag.addStream("Kinesis message", kinesisInput.outputPort, s3OutputOperator.input).setLocality(Locality.CONTAINER_LOCAL);
  }

}

