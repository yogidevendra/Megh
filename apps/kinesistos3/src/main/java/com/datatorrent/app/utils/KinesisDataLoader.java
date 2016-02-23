package com.datatorrent.app.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

public class KinesisDataLoader
{
  public static void main(String[] args)
  {
    AWSCredentials credentials = new BasicAWSCredentials("AKIAIY62OEFADWS65JZA", "CQeMB8vVgkOh6JnyY0hfq643RMTBhvRQFRtok5aa");
    AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(credentials);
    PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
    putRecordsRequest.setStreamName("TestStream");
    for (int recordBatch = 0; recordBatch < 1000; recordBatch++) {
      List <PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>(); 
      for (int i = 0; i < 100; i++) {
          PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
          putRecordsRequestEntry.setData(ByteBuffer.wrap("ABCDEFGHIJKLMNOPQRSTUVWXYZ|ABCDEFGHIJKLMNOPQRSTUVWXYZ$ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes()));
          putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
          putRecordsRequestEntryList.add(putRecordsRequestEntry); 
      }

      putRecordsRequest.setRecords(putRecordsRequestEntryList);
      PutRecordsResult putRecordsResult  = amazonKinesisClient.putRecords(putRecordsRequest);
      System.out.println("Put Result" + putRecordsResult);
    }
    
  }

}
