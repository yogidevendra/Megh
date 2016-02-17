/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.ingestion.io.input;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.commons.lang3.tuple.MutablePair;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.apps.ingestion.io.BandwidthLimitingOperator;
import com.datatorrent.apps.ingestion.lib.BandwidthManager;
import com.datatorrent.apps.ingestion.lib.BandwidthPartitioner;
import com.datatorrent.contrib.kafka.AbstractKafkaInputOperator;
import com.datatorrent.contrib.kafka.AbstractKafkaSinglePortInputOperator;
import com.datatorrent.contrib.kafka.KafkaConsumer;

import kafka.message.Message;

  /**
   * <p>KafkaSinglePortByteArrayInputOperator class.</p>
   *
   * @since 2.1.0
   */
  public class KafkaSinglePortByteArrayInputOperator extends AbstractKafkaSinglePortInputOperator<byte[]> implements BandwidthLimitingOperator
  {
    private BandwidthManager bandwidthManager;

    @AutoMetric
    private long inputMessagesPerSec;
    
    @AutoMetric
    private long inputBytesPerSec;
    
    private long messageCount;
    private long byteCount;
    private double windowTimeSec; 

    public KafkaSinglePortByteArrayInputOperator()
    {
      bandwidthManager = new BandwidthManager();
    }

    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      bandwidthManager.setup(context);
      windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
    }
    
    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      inputMessagesPerSec = 0;
      inputBytesPerSec = 0;
      messageCount = 0;
      byteCount = 0;
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      inputBytesPerSec = (long) (byteCount / windowTimeSec);
      inputMessagesPerSec = (long) (messageCount / windowTimeSec);
    }

    @Override
    public Collection<Partition<AbstractKafkaInputOperator<KafkaConsumer>>> definePartitions(Collection<Partition<AbstractKafkaInputOperator<KafkaConsumer>>> partitions, PartitioningContext context)
    {
      KafkaSinglePortByteArrayInputOperator currentPartition = (KafkaSinglePortByteArrayInputOperator) partitions.iterator().next().getPartitionedInstance();
      long currentBandwidth = currentPartition.getBandwidthManager().getBandwidth() * partitions.size();
      Collection<Partition<AbstractKafkaInputOperator<KafkaConsumer>>> newPartitions =  super.definePartitions(partitions, context);
      new BandwidthPartitioner().updateBandwidth(newPartitions, currentBandwidth);
      return newPartitions;
    }

    @Override
    public void emitTuples()
    {
      if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
        return;
      }
      int sendCount = consumer.messageSize();
      if (getMaxTuplesPerWindow() > 0) {
        sendCount = Math.min(sendCount, getMaxTuplesPerWindow() - (int)messageCount);
      }
      for (int i = 0; i < sendCount; i++) {
        if(!bandwidthManager.canConsumeBandwidth()) {
          break;
        }
        KafkaConsumer.KafkaMessage message = consumer.pollMessage();
        // Ignore the duplicate messages
        if(offsetStats.containsKey(message.getKafkaPart()) && message.getOffSet() <= offsetStats.get(message.getKafkaPart())) {
          continue;
        }
        emitTuple(message.getMsg());
        bandwidthManager.consumeBandwidth(message.getMsg().size());
        offsetStats.put(message.getKafkaPart(), message.getOffSet());
        MutablePair<Long, Integer> offsetAndCount = currentWindowRecoveryState.get(message.getKafkaPart());
        if(offsetAndCount == null) {
          currentWindowRecoveryState.put(message.getKafkaPart(), new MutablePair<Long, Integer>(message.getOffSet(), 1));
        } else {
          offsetAndCount.setRight(offsetAndCount.right+1);
        }
      }
    }

    /**
     * Implement abstract method of AbstractKafkaSinglePortInputOperator
     *
     * @param message
     * @return byte Array
     */
    @Override
    public byte[] getTuple(Message message)
    {
      byte[] bytes = null;
      try {
        messageCount++;
        ByteBuffer buffer = message.payload();
        bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        byteCount += bytes.length;
      }
      catch (Exception ex) {
        return bytes;
      }
      return bytes;
    }

    @Override
    public void teardown()
    {
      super.teardown();
      bandwidthManager.teardown();
    }

    @Override
    public BandwidthManager getBandwidthManager()
    {
      return bandwidthManager;
    }

    public void setBandwidthManager(BandwidthManager bandwidthManager)
    {
      this.bandwidthManager = bandwidthManager;
    }
  }
