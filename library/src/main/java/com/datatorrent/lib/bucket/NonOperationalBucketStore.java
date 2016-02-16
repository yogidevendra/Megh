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
import java.util.Map;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.api.annotation.Stateless;

/**
 * {@link BucketStore} which doesn't keep any state.<br/>
 *
 * @param <T> type of bucket event
 * @since 1.0.1
 */
@Stateless
public class NonOperationalBucketStore<T> implements BucketStore.ExpirableBucketStore<T>
{
  public NonOperationalBucketStore()
  {
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setNoOfBuckets(int noOfBuckets)
  {
  }

  @Override
  public void setWriteEventKeysOnly(boolean writeEventKeysOnly)
  {
  }

  @Override
  public void setup()
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void teardown()
  {
  }

  /**
   * {@inheritDoc}
   * @throws java.io.IOException
   */
  @Override
  public void storeBucketData(long window, long timestamp, Map<Integer, Map<Object, T>> data) throws IOException
  {
  }

  /**
   * {@inheritDoc}
   * @throws java.io.IOException
   */
  @Override
  public void deleteBucket(int bucketIdx) throws IOException
  {
  }

  /**
   * {@inheritDoc}
   * @throws java.lang.Exception
   */
  @Override
  @Nonnull
  public Map<Object, T> fetchBucket(int bucketIdx) throws Exception
  {
    return Maps.newHashMap();
  }

  @Override
  public void deleteExpiredBuckets(long time) throws IOException
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
  }

  @Override
  public void captureFilesToDelete(long windowId)
  {
  }

  @Override
  @SuppressWarnings("unchecked")
  public NonOperationalBucketStore<T> clone() throws CloneNotSupportedException
  {
    return (NonOperationalBucketStore<T>)super.clone();
  }

  private static transient final Logger logger = LoggerFactory.getLogger(NonOperationalBucketStore.class);
}