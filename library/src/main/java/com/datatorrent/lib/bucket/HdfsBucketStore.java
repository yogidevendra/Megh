/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.bucket;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.*;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import com.datatorrent.common.util.NameableThreadFactory;

/**
 * {@link BucketStore} which works with HDFS.<br/>
 * The path of buckets in hdfs is <code>{application-path}/buckets/{operatorId}/{windowId}</code>.
 *
 * @param <T> type of bucket event
 * @since 0.9.4
 */
public class HdfsBucketStore<T> implements BucketStore<T>
{
  public static transient String OPERATOR_ID = "operatorId";
  public static transient String STORE_ROOT = "storeRoot";
  public static transient String PARTITION_KEYS = "partitionKeys";
  public static transient String PARTITION_MASK = "partitionMask";
  public static transient int DEF_CORE_POOL_SIZE = 10;
  public static transient int DEF_KEEP_ALIVE_SECONDS = 120;

  static transient final String PATH_SEPARATOR = "/";

  //Check-pointed
  private boolean writeEventKeysOnly;
  @Min(1)
  protected int noOfBuckets;
  protected Map<Long, Long>[] bucketPositions;
  protected Map<Long, Long> windowToTimestamp;
  protected Class<?> eventKeyClass;
  protected Class<T> eventClass;
  protected int corePoolSize;
  protected int maximumPoolSize;
  protected int keepAliveSeconds;
  protected int hardLimitOnPoolSize;
  protected int interpolatedPoolSize;
  @NotNull
  private String bucketsDir;

  //Non check-pointed
  protected transient Multimap<Long, Integer> windowToBuckets;
  protected transient String bucketRoot;
  protected transient Configuration configuration;
  protected transient Kryo writeSerde;
  protected transient ClassLoader classLoader;
  protected transient Set<Integer> partitionKeys;
  protected transient int partitionMask;
  protected transient int operatorId;
  protected transient ThreadPoolExecutor threadPoolExecutor;

  public HdfsBucketStore()
  {
    windowToTimestamp = Maps.newHashMap();
    corePoolSize = DEF_CORE_POOL_SIZE;
    maximumPoolSize = -1;
    interpolatedPoolSize = -1;
    keepAliveSeconds = DEF_KEEP_ALIVE_SECONDS;
    bucketsDir = "buckets";
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setNoOfBuckets(int noOfBuckets)
  {
    this.noOfBuckets = noOfBuckets;
    bucketPositions = (Map<Long, Long>[]) Array.newInstance(HashMap.class, noOfBuckets);
  }

  @Override
  public void setWriteEventKeysOnly(boolean writeEventKeysOnly)
  {
    this.writeEventKeysOnly = writeEventKeysOnly;
  }

  public void setCorePoolSize(int corePoolSize)
  {
    this.corePoolSize = corePoolSize;
  }

  public void setMaximumPoolSize(int maximumPoolSize)
  {
    this.maximumPoolSize = maximumPoolSize;
  }

  public void setKeepAliveSeconds(int keepAliveSeconds)
  {
    this.keepAliveSeconds = keepAliveSeconds;
  }

  public void setHardLimitOnPoolSize(int hardLimitOnPoolSize)
  {
    this.hardLimitOnPoolSize = hardLimitOnPoolSize;
  }

  public void setBucketsDir(@Nonnull String bucketsDir)
  {
    this.bucketsDir = bucketsDir;
  }

  public void setConfiguration(int operatorId, String applicationPath, Set<Integer> partitionKeys, int partitionMask)
  {
    Preconditions.checkNotNull(applicationPath);
    this.operatorId = operatorId;
    this.bucketRoot = applicationPath + PATH_SEPARATOR + bucketsDir + PATH_SEPARATOR + operatorId;
    this.partitionKeys = Preconditions.checkNotNull(partitionKeys, "partition keys");
    this.partitionMask = partitionMask;
    logger.debug("operator parameters {}, {}, {}", operatorId, partitionKeys, partitionMask);
  }

  @Override
  public void setup()
  {
    this.configuration = new Configuration();
    this.writeSerde = new Kryo();
    classLoader = Thread.currentThread().getContextClassLoader();
    this.writeSerde.setClassLoader(classLoader);
    if (logger.isDebugEnabled()) {
      for (int i = 0; i < bucketPositions.length; i++) {
        if (bucketPositions[i] != null) {
          logger.debug("bucket idx {} position {}", i, bucketPositions[i]);
        }
      }
    }
    windowToBuckets = ArrayListMultimap.create();
    for (int i = 0; i < bucketPositions.length; i++) {
      if (bucketPositions[i] != null) {
        for (Long window : bucketPositions[i].keySet()) {
          windowToBuckets.put(window, i);
        }
      }
    }
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
    NameableThreadFactory threadFactory = new NameableThreadFactory("BucketFetchFactory");
    if (maximumPoolSize == -1) {
      interpolatedPoolSize = corePoolSize;
      threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, interpolatedPoolSize, keepAliveSeconds, TimeUnit.SECONDS, queue, threadFactory);
    }
    else {
      threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveSeconds, TimeUnit.SECONDS, queue, threadFactory);
    }
    logger.debug("threadpool settings {} {} {}", threadPoolExecutor.getCorePoolSize(), threadPoolExecutor.getMaximumPoolSize(), keepAliveSeconds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void teardown()
  {
    //Not closing the filesystem.
    threadPoolExecutor.shutdown();
    configuration.clear();
  }

  /**
   * {@inheritDoc}
   * @throws java.io.IOException
   */
  @Override
  public void storeBucketData(long window, long timestamp, Map<Integer, Map<Object, T>> data) throws IOException
  {
    Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + window);
    FileSystem fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
    FSDataOutputStream dataStream = fs.create(dataFilePath);

    Output output = new Output(dataStream);
    try {
      long offset = 0;
      for (int bucketIdx : data.keySet()) {
        Map<Object, T> bucketData = data.get(bucketIdx);

        if (eventKeyClass == null) {
          Map.Entry<Object, T> eventEntry = bucketData.entrySet().iterator().next();
          eventKeyClass = eventEntry.getKey().getClass();
          if (!writeEventKeysOnly) {
            @SuppressWarnings("unchecked")
            Class<T> lEventClass = (Class<T>) eventEntry.getValue().getClass();
            eventClass = lEventClass;
          }
        }
        //Write the size of data and then data
        dataStream.writeInt(bucketData.size());
        for (Map.Entry<Object, T> entry : bucketData.entrySet()) {
          writeSerde.writeObject(output, entry.getKey());

          if (!writeEventKeysOnly) {
            int posLength = output.position();
            output.writeInt(0); //temporary place holder
            writeSerde.writeObject(output, entry.getValue());
            int posValue = output.position();
            int valueLength = posValue - posLength - 4;
            output.setPosition(posLength);
            output.writeInt(valueLength);
            output.setPosition(posValue);
          }
        }
        output.flush();
        if (bucketPositions[bucketIdx] == null) {
          bucketPositions[bucketIdx] = Maps.newHashMap();
        }
        windowToBuckets.put(window, bucketIdx);
        windowToTimestamp.put(window, timestamp);
        synchronized (bucketPositions[bucketIdx]) {
          bucketPositions[bucketIdx].put(window, offset);
        }
        offset = dataStream.getPos();
      }
    }
    finally {
      output.close();
      dataStream.close();
      fs.close();
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws java.io.IOException
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void deleteBucket(int bucketIdx) throws IOException
  {
    Map<Long, Long> offsetMap = bucketPositions[bucketIdx];
    if (offsetMap != null) {
      for (Long window : offsetMap.keySet()) {
        Collection<Integer> indices = windowToBuckets.get(window);
        synchronized (indices) {
          boolean elementRemoved = indices.remove(bucketIdx);
          if (indices.isEmpty() && elementRemoved) {
            deleteFile(window.toString());
            windowToBuckets.removeAll(window);
            windowToTimestamp.remove(window);
          }
        }
      }
    }
    bucketPositions[bucketIdx] = null;
  }

  protected void deleteFile(String fileName) throws IOException
  {
    Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + fileName);
    try (FileSystem fs = FileSystem.newInstance(dataFilePath.toUri(), configuration)) {
      if (fs.exists(dataFilePath)) {
        logger.debug("start delete {}", fileName);
        fs.delete(dataFilePath, true);
        logger.debug("end delete {}", fileName);
      }
    }
  }

  /**
   * {@inheritDoc}
   * @throws java.lang.Exception
   */
  @Override
  @Nonnull
  public Map<Object, T> fetchBucket(int bucketIdx) throws Exception
  {
    Map<Object, T> bucketData = Maps.newHashMap();

    if (bucketPositions[bucketIdx] == null) {
      return bucketData;
    }

    logger.debug("start fetch bucket {}", bucketIdx);

    long startTime = System.currentTimeMillis();
    Set<Long> windows = bucketPositions[bucketIdx].keySet();
    int numWindows = windows.size();
    if (maximumPoolSize == -1 && interpolatedPoolSize < numWindows && interpolatedPoolSize < hardLimitOnPoolSize) {
      int diff = numWindows - interpolatedPoolSize;
      if (interpolatedPoolSize + diff <= hardLimitOnPoolSize) {
        interpolatedPoolSize += diff;
      }
      else {
        interpolatedPoolSize = hardLimitOnPoolSize;
      }
      logger.debug("interpolated pool size {}", interpolatedPoolSize);
      threadPoolExecutor.setMaximumPoolSize(interpolatedPoolSize);
    }

    List<Future<Exchange<T>>> futures = Lists.newArrayList();
    for (long window : windows) {
      futures.add(threadPoolExecutor.submit(new BucketFetchCallable(bucketIdx, window)));
    }

    if (writeEventKeysOnly) {
      for (Future<Exchange<T>> future : futures) {
        bucketData.putAll(future.get().data);
      }
    }
    else {
      List<Exchange<T>> holder = Lists.newArrayList();
      for (Future<Exchange<T>> future : futures) {
        holder.add(future.get());
      }
      Collections.sort(holder);
      for (Exchange<T> hdata : holder) {
        bucketData.putAll(hdata.data);
      }
    }
    logger.debug("end fetch bucket {} num {} took {}", bucketIdx, bucketData.size(), System.currentTimeMillis() - startTime);
    return bucketData;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HdfsBucketStore)) {
      return false;
    }

    HdfsBucketStore<?> that = (HdfsBucketStore<?>) o;

    if (noOfBuckets != that.noOfBuckets) {
      return false;
    }
    if (writeEventKeysOnly != that.writeEventKeysOnly) {
      return false;
    }
    return Arrays.equals(bucketPositions, that.bucketPositions);

  }

  @Override
  public int hashCode()
  {
    int result = (writeEventKeysOnly ? 1 : 0);
    result = 31 * result + noOfBuckets;
    result = 31 * result + (bucketPositions != null ? Arrays.hashCode(bucketPositions) : 0);
    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public HdfsBucketStore<T> clone() throws CloneNotSupportedException
  {
    return (HdfsBucketStore<T>)super.clone();
  }

  private class Exchange<E> implements Comparable<Exchange<E>>
  {
    final long window;
    final Map<Object, E> data;

    Exchange(long window, Map<Object, E> data)
    {
      this.window = window;
      this.data = data;
    }

    @Override
    public int compareTo(@Nonnull Exchange<E> tExchange)
    {
      return (int) (window - tExchange.window);
    }
  }

  private class BucketFetchCallable implements Callable<Exchange<T>>
  {

    final long window;
    final int bucketIdx;

    BucketFetchCallable(int bucketIdx, long window)
    {
      this.bucketIdx = bucketIdx;
      this.window = window;
    }

    @Override
    public Exchange<T> call() throws IOException
    {
      Kryo readSerde = new Kryo();
      readSerde.setClassLoader(classLoader);

      Map<Object, T> bucketDataPerWindow = Maps.newHashMap();
      Path dataFile = new Path(bucketRoot + PATH_SEPARATOR + window);
      FileSystem fs = FileSystem.newInstance(dataFile.toUri(), configuration);
      try {
        //Read data only for the fileIds in which bucketIdx had events.
        FSDataInputStream stream = fs.open(dataFile);
        stream.seek(bucketPositions[bucketIdx].get(window));
        Input input = new Input(stream);

        int length = stream.readInt();

        for (int i = 0; i < length; i++) {
          Object key = readSerde.readObject(input, eventKeyClass);

          int partitionKey = key.hashCode() & partitionMask;
          boolean keyPasses = partitionKeys.contains(partitionKey);

          if (!writeEventKeysOnly) {
            //if key passes then read the value otherwise skip the value
            int entrySize = input.readInt();
            if (keyPasses) {
              T entry = readSerde.readObject(input, eventClass);
              bucketDataPerWindow.put(key, entry);
            }
            else {
              input.skip(entrySize);
            }
          }
          else if (keyPasses) {
            bucketDataPerWindow.put(key, null);
          }
        }
        input.close();
        stream.close();
      }
      finally {
        fs.close();
      }
      return new Exchange<T>(window, bucketDataPerWindow);
    }
  }

  private static transient final Logger logger = LoggerFactory.getLogger(HdfsBucketStore.class);
}