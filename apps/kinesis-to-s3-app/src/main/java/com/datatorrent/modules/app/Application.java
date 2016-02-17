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
import com.datatorrent.contrib.kinesis.KinesisStringInputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;


@ApplicationAnnotation(name = "KinesisToS3")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    AWSCredentials credentials = new BasicAWSCredentials("AKIAIY62OEFADWS65JZA", "CQeMB8vVgkOh6JnyY0hfq643RMTBhvRQFRtok5aa");
    
 // Create KinesisSinglePortStringInputOperator
    KinesisStringInputOperator node = dag.addOperator("KinesisMessageConsumer", KinesisStringInputOperator.class);
    node.setAccessKey(credentials.getAWSSecretKey());
    node.setSecretKey(credentials.getAWSAccessKeyId());
    KinesisConsumer consumer = new KinesisConsumer();
    consumer.setStreamName("TestStream");
    consumer.setRecordsLimit(100);
    node.setConsumer(consumer);

    // Create Test tuple collector
    ConsoleOutputOperator collector = dag.addOperator("console", new ConsoleOutputOperator());

    // Connect ports
    dag.addStream("Kinesis message", node.outputPort, collector.input).setLocality(Locality.CONTAINER_LOCAL);
  }

}

