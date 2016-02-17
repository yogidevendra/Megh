
/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 *
 * @since 1.0.0
 */
package com.datatorrent.apps.ingestion.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Stats;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.malhar.lib.io.block.AbstractBlockReader;
import com.datatorrent.malhar.lib.io.block.BlockMetadata;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@StatsListener.DataQueueSize
public class ReaderWriterPartitioner implements Partitioner<BlockReader>, StatsListener, Serializable
{
  //should be power of 2
  private final int maxPartition;

  //should be power of 2
  private final int minPartition;
  /**
   * Interval at which stats are processed. Default : 10 seconds
   */
  private long intervalMillis;

  private int partitionCount;

  private final StatsListener.Response readerResponse;

  private final StatsListener.Response writerResponse;

  private final StatsListener.Response splitterResponse;

  private final Map<Integer, Long> readerBacklog;

  private final Map<Integer, Long> writerBacklog;

  protected final Map<Integer, BasicCounters<MutableLong>> readerCounters;

  private transient long readBandwidth;

  private transient long nextMillis;

  private transient int splitterThreshold;
  private transient int changedThreshold;

  public ReaderWriterPartitioner()
  {
    readerResponse = new StatsListener.Response();
    writerResponse = new StatsListener.Response();
    splitterResponse = new StatsListener.Response();

    readerBacklog = Maps.newHashMap();
    writerBacklog = Maps.newHashMap();

    readerCounters = Maps.newHashMap();

    partitionCount = 1;
    maxPartition = 16;
    minPartition = 1;
    intervalMillis = 10000L; //10 seconds
  }

  @Override
  public Collection<Partition<BlockReader>> definePartitions(Collection<Partition<BlockReader>> collection,
                                                             PartitioningContext partitioningContext)
  {
    //sync the stats listener properties with the operator
    Partition<BlockReader> readerPartition = collection.iterator().next();

    //changes max throughput on the partitioner
    if (readBandwidth != readerPartition.getPartitionedInstance().bandwidth) {
      LOG.debug("readBandwidth: from {} to {}", readBandwidth, readerPartition.getPartitionedInstance().bandwidth);
      readBandwidth = readerPartition.getPartitionedInstance().bandwidth;
    }

    if (readerPartition.getStats() == null) {
      //First time when define partitions is called, no partitioning required
      return collection;
    }

    int morePartitionsToCreate = partitionCount - collection.size();
    List<BasicCounters<MutableLong>> deletedCounters = Lists.newArrayList();

    if (morePartitionsToCreate < 0) {
      //Delete partitions
      Iterator<Partition<BlockReader>> partitionIterator = collection.iterator();
      while (morePartitionsToCreate++ < 0) {
        Partition<BlockReader> toRemove = partitionIterator.next();
        deletedCounters.add(toRemove.getPartitionedInstance().getCounters());

        LOG.debug("partition removed {}", toRemove.getPartitionedInstance().getOperatorId());
        partitionIterator.remove();
      }
    }
    else {
      //Add more partitions
      Kryo kryo = new Kryo();
      while (morePartitionsToCreate-- > 0) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output loutput = new Output(bos);
        kryo.writeObject(loutput, readerPartition.getPartitionedInstance());
        loutput.close();
        Input lInput = new Input(bos.toByteArray());

        BlockReader blockReader = kryo.readObject(lInput, BlockReader.class);

        DefaultPartition<BlockReader> partition = new DefaultPartition<BlockReader>(blockReader);
        collection.add(partition);
      }
    }
    @SuppressWarnings("unchecked")
    DefaultInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = (DefaultInputPort<BlockMetadata
      .FileBlockMetadata>) partitioningContext.getInputPorts().get(0);

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(collection), blocksMetadataInput);
    int lPartitionMask = collection.iterator().next().getPartitionKeys().get(blocksMetadataInput).mask;

    //transfer the state here
    for (Partition<BlockReader> newPartition : collection) {
      BlockReader reader = newPartition.getPartitionedInstance();

      reader.setPartitionKeys(newPartition.getPartitionKeys().get(blocksMetadataInput).partitions);
      reader.setPartitionMask(lPartitionMask);
      LOG.debug("partitions {},{}", reader.getPartitionKeys(), reader.getPartitionMask());
    }

    //transfer the counters
    BlockReader targetReader = collection.iterator().next().getPartitionedInstance();
    for (BasicCounters<MutableLong> removedCounter : deletedCounters) {
      addCounters(targetReader.getCounters(), removedCounter);
    }

    return collection;
  }

  protected void addCounters(BasicCounters<MutableLong> target, BasicCounters<MutableLong> source)
  {

    for (Enum<AbstractBlockReader.ReaderCounterKeys> key : AbstractBlockReader.ReaderCounterKeys.values()) {
      MutableLong tcounter = target.getCounter(key);
      if (tcounter == null) {
        tcounter = new MutableLong();
        target.setCounter(key, tcounter);
      }
      MutableLong scounter = source.getCounter(key);
      if (scounter != null) {
        tcounter.add(scounter.longValue());
      }
    }
  }

  @Override
  public void partitioned(Map<Integer, Partition<BlockReader>> map)
  {
    //Placeholder for custom code
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    readerResponse.repartitionRequired = false;
    List<Stats.OperatorStats> lastWindowedStats = stats.getLastWindowedStats();
    ListenerType type = null;

    if (lastWindowedStats != null && lastWindowedStats.size() > 0) {
      for (int i = lastWindowedStats.size() - 1; i >= 0; i--) {
        Object counters = lastWindowedStats.get(i).counters;

        if (counters != null) {
          @SuppressWarnings("unchecked")
          BasicCounters<MutableLong> lcounters = (BasicCounters<MutableLong>) counters;
          if (lcounters.getCounter(BlockReader.ReaderCounterKeys.BYTES) != null) {
            type = ListenerType.READER;
            readerBacklog.put(stats.getOperatorId(), (long) lastWindowedStats.get(i).inputPorts.get(0).queueSize);
            readerCounters.put(stats.getOperatorId(), lcounters);
          }
          else if (lcounters.getCounter(BlockWriter.Counters.TOTAL_BYTES_WRITTEN) != null) {
            type = ListenerType.WRITER;
            writerBacklog.put(stats.getOperatorId(), (long) lastWindowedStats.get(i).inputPorts.get(0).queueSize);
          }
          else if (lcounters.getCounter(IngestionFileSplitter.PropertyCounters.THRESHOLD) != null) {
            type = ListenerType.SPLITTER;
            splitterThreshold = lcounters.getCounter(IngestionFileSplitter.PropertyCounters.THRESHOLD).intValue();
          }
          break;
        }
      }
    }

    if (type == ListenerType.WRITER) {
      return writerResponse;
    }
    if (type == ListenerType.SPLITTER) {
      if (changedThreshold > splitterThreshold) {
        LOG.debug("splitter threshold {} to {}", splitterThreshold, changedThreshold);
        splitterResponse.operatorRequests = Lists.newArrayList(new SetThresholdRequest(changedThreshold));
        changedThreshold = 0;
      }
      return splitterResponse;
    }
    if (System.currentTimeMillis() < nextMillis) {
      return readerResponse;
    }

    //check if partitioning is needed after stats from all the operators are received.
    if (readerBacklog.size() >= partitionCount && writerBacklog.size() >= partitionCount && splitterThreshold > 0) {

      nextMillis = System.currentTimeMillis() + intervalMillis;
      LOG.debug("Proposed NextMillis = {}", nextMillis);

      long totalReaderBacklog = 0;
      for (Map.Entry<Integer, Long> backlog : readerBacklog.entrySet()) {
        totalReaderBacklog += backlog.getValue();
      }

      long totalReadBytes = getTotalOf(readerCounters, BlockReader.ReaderCounterKeys.BYTES);
      long totalReadTime = getTotalOf(readerCounters, BlockReader.ReaderCounterKeys.TIME);

      LOG.debug("reader total: bytes {} time {}", totalReadBytes, totalReadTime);
      if (totalReadTime == 0) {
        //Reader didn't do any work so return.
        clearState();
        return readerResponse;
      }
      long bytesReadPerSec = totalReadBytes / totalReadTime;

      long totalWriterBacklog = 0;
      for (Map.Entry<Integer, Long> backlog : writerBacklog.entrySet()) {
        totalWriterBacklog += backlog.getValue();
      }
      long backlogConsidered = Math.max(totalReaderBacklog, totalWriterBacklog);
      LOG.debug("total backlog: reader {} writer {} max {}, partition: {}", totalReaderBacklog, totalWriterBacklog,
        backlogConsidered, partitionCount);

      if (bytesReadPerSec < readBandwidth && backlogConsidered == 0) {
        changedThreshold = splitterThreshold + 1;
      }

      if (backlogConsidered <= 0) {
        //no backlog so scale down completely
        LOG.debug("no backlog");
        if (partitionCount > minPartition) {
          LOG.debug("partition change to {}", minPartition);
          partitionCount = minPartition;
          readerResponse.repartitionRequired = true;
          clearState();
          return readerResponse;
        }
      }
      else if (backlogConsidered == partitionCount) {
        clearState();
        return readerResponse; //do not repartition
      }
      else {
        // backlog exists and either we scale down or up to address it.
        int newPartitionCount;
        if (backlogConsidered < partitionCount) {
          newPartitionCount = (int) backlogConsidered;
        }
        else {

          int newCountByThroughput = partitionCount + (int) ((readBandwidth - bytesReadPerSec) /
            (bytesReadPerSec / partitionCount));

          LOG.debug("countByThroughput {}", newCountByThroughput);
          if (readBandwidth > 0 && newCountByThroughput < partitionCount) {
            //can't scale up since throughput limit is reached.
            newPartitionCount = partitionCount;
          }
          else if (readBandwidth > 0 && newCountByThroughput <= maxPartition) {
            newPartitionCount = (backlogConsidered > newCountByThroughput) ? newCountByThroughput : (int) backlogConsidered;
          }
          else {
            LOG.debug("byMaxPartition {}", maxPartition);
            newPartitionCount = backlogConsidered > maxPartition ? maxPartition : (int) backlogConsidered;
          }
        }
        clearState();

        if (newPartitionCount == partitionCount) {
          return readerResponse; //do not repartition
        }

        //partition count can only be a power of 2. so adjusting newPartitionCount if it isn't
        newPartitionCount = getAdjustedCount(newPartitionCount);

        partitionCount = newPartitionCount;
        readerResponse.repartitionRequired = true;
        LOG.debug("end listener {} {}", totalReaderBacklog, partitionCount);
        return readerResponse;
      }
    }
    return readerResponse;
  }

  private void clearState()
  {
    readerBacklog.clear();
    writerBacklog.clear();
    readerCounters.clear();
  }

  protected int getAdjustedCount(int newCount)
  {
    Preconditions.checkArgument(newCount <= maxPartition && newCount >= minPartition, newCount);

    int adjustCount = 1;
    while (adjustCount < newCount) {
      adjustCount <<= 1;
    }
    if (adjustCount > newCount) {
      adjustCount >>>= 1;
    }
    LOG.debug("adjust {} => {}", newCount, adjustCount);
    return adjustCount;
  }

  private long getTotalOf(Map<Integer, BasicCounters<MutableLong>> countersMap, Enum<?> key)
  {
    long total = 0;
    for (BasicCounters<MutableLong> counters : countersMap.values()) {
      total += counters.getCounter(key).longValue();
    }
    return total;
  }
  @SuppressWarnings("unused")
  private long getMinOf(Map<Integer, BasicCounters<MutableLong>> countersMap, Enum<?> key)
  {
    long min = -1;
    for (BasicCounters<MutableLong> counters : countersMap.values()) {
      long val = counters.getCounter(key).longValue();
      if (min == -1 || val < min) {
        min = val;
      }
    }
    return min;
  }

  @SuppressWarnings("unused")
  private long getMaxOf(Map<Integer, BasicCounters<MutableLong>> countersMap, Enum<?> key)
  {
    long max = -1;
    for (BasicCounters<MutableLong> counters : countersMap.values()) {
      long val = counters.getCounter(key).longValue();
      if (val > max) {
        max = val;
      }
    }
    return max;
  }

  @VisibleForTesting
  Response getResponse()
  {
    return readerResponse;
  }

  @VisibleForTesting
  int getMaxPartition()
  {
    return maxPartition;
  }

  @VisibleForTesting
  int getMinPartition()
  {
    return minPartition;
  }

  @VisibleForTesting
  int getPartitionCount()
  {
    return partitionCount;
  }

  @VisibleForTesting
  void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  @VisibleForTesting
  long getReadBandwidth()
  {
    return readBandwidth;
  }

  @VisibleForTesting
  void setReadBandwidth(long throughput)
  {
    this.readBandwidth = throughput;
  }

  public void setIntervalMillis(long millis)
  {
    this.intervalMillis = millis;
  }

  public long getIntervalMillis()
  {
    return intervalMillis;
  }

  private static enum ListenerType
  {
    SPLITTER, READER, WRITER
  }

  private static class SetThresholdRequest implements OperatorRequest, Serializable
  {
    private final int threshold;

    public SetThresholdRequest(int threshold)
    {
      this.threshold = threshold;
    }

    @Override
    public OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
    {
      if (operator instanceof IngestionFileSplitter) {
        ((IngestionFileSplitter) operator).setBlocksThreshold(threshold);
      }
      return null;
    }

    private static final long serialVersionUID = 201503231644L;
  }

  private static final long serialVersionUID = 201502130023L;

  private static final Logger LOG = LoggerFactory.getLogger(ReaderWriterPartitioner.class);

}
