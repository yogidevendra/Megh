/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.Map;
import java.util.Set;


import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.QueryBundle;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.FieldsAggregatable;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.util.TestUtils.TestInfo;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHTTest.InterruptClear;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHTTest.StoreFSTestWatcher;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;

public class DimensionsQueueManagerTest
{
  @Rule
  public TestInfo testMeta = new StoreFSTestWatcher();

  @Rule
  public TestWatcher interruptClear = new InterruptClear();

  @Test
  public void simpleQueueManagerTest() throws Exception
  {
    final int numQueries = 3;

    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setCacheWindowDuration(2);
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.configurationSchema;
    DimensionsQueueManager dqm = new DimensionsQueueManager(store, store.schemaRegistry);

    Map<String, Set<String>> fieldToAggregator = Maps.newHashMap();
    fieldToAggregator.put("impressions", Sets.newHashSet("SUM"));
    fieldToAggregator.put("cost", Sets.newHashSet("SUM"));

    FieldsAggregatable fieldsAggregatable = new FieldsAggregatable(fieldToAggregator);

    GPOMutable key = AppDataSingleSchemaDimensionStoreHDHTTest.createQueryKey(eventSchema,
                                                                              "google",
                                                                              "safeway");

    DataQueryDimensional dqd = new DataQueryDimensional("1",
                                                        DataQueryDimensional.TYPE,
                                                        numQueries,
                                                        TimeBucket.MINUTE,
                                                        key,
                                                        fieldsAggregatable,
                                                        true);

    LOG.debug("{}", dqd.getDimensionsDescriptor());
    LOG.debug("{}", ((DimensionalSchema)store.schemaRegistry.getSchema(dqd.getSchemaKeys())).getDimensionalConfigurationSchema().getDimensionsDescriptorToID());

    dqm.enqueue(dqd, null, null);

    Assert.assertEquals(numQueries, store.getQueries().size());
  }

  @Test
  public void simpleQueueManagerTestCustomTimeBucket() throws Exception
  {
    final int numQueries = 3;

    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchemaCustomTimeBucket.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setCacheWindowDuration(2);
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);
    store.setUseSystemTimeForLatestTimeBuckets(false);
    store.setMinTimestamp(600000L);
    store.setMaxTimestamp(1000000L);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.configurationSchema;
    DimensionsQueueManager dqm = new DimensionsQueueManager(store, store.schemaRegistry);

    Map<String, Set<String>> fieldToAggregator = Maps.newHashMap();
    fieldToAggregator.put("impressions", Sets.newHashSet("SUM"));
    fieldToAggregator.put("cost", Sets.newHashSet("SUM"));

    FieldsAggregatable fieldsAggregatable = new FieldsAggregatable(fieldToAggregator);

    GPOMutable key = AppDataSingleSchemaDimensionStoreHDHTTest.createQueryKey(eventSchema,
                                                                              "google",
                                                                              "safeway");

    DataQueryDimensional dqd = new DataQueryDimensional("1",
                                                        DataQueryDimensional.TYPE,
                                                        numQueries,
                                                        TimeBucket.MINUTE,
                                                        key,
                                                        fieldsAggregatable,
                                                        true);

    LOG.debug("{}", dqd.getDimensionsDescriptor());
    LOG.debug("{}", ((DimensionalSchema)store.schemaRegistry.getSchema(dqd.getSchemaKeys())).getDimensionalConfigurationSchema().getDimensionsDescriptorToID());

    dqm.enqueue(dqd, null, null);

    QueryBundle<DataQueryDimensional, QueryMeta, MutableLong> qb = dqm.dequeue();

    for (Map<String, EventKey> eventKeys : qb.getMetaQuery().getEventKeys()) {
      Assert.assertEquals(0, eventKeys.get("SUM").getDimensionDescriptorID());
    }

    Assert.assertEquals(numQueries, store.getQueries().size());
  }

  @Test
  public void simpleRollingQueueManagerTest() throws Exception
  {
    final int numQueries = 3;
    final int rollingCount = 5;
    final int hdhtQueryCount = 7;

    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setCacheWindowDuration(2);
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.configurationSchema;
    DimensionsQueueManager dqm = new DimensionsQueueManager(store, store.schemaRegistry);

    Map<String, Set<String>> fieldToAggregator = Maps.newHashMap();
    fieldToAggregator.put("impressions", Sets.newHashSet("SUM"));
    fieldToAggregator.put("cost", Sets.newHashSet("SUM"));

    FieldsAggregatable fieldsAggregatable = new FieldsAggregatable(fieldToAggregator);

    GPOMutable key = AppDataSingleSchemaDimensionStoreHDHTTest.createQueryKey(eventSchema,
                                                                              "google",
                                                                              "safeway");

    DataQueryDimensional dqd = new DataQueryDimensional("1",
                                                        DataQueryDimensional.TYPE,
                                                        numQueries,
                                                        TimeBucket.MINUTE,
                                                        key,
                                                        fieldsAggregatable,
                                                        true);
    dqd.setSlidingAggregateSize(rollingCount);

    dqm.enqueue(dqd, null, null);

    Assert.assertEquals(hdhtQueryCount, store.getQueries().size());
  }

  @Test
  public void queryStarQueueManagerTest() throws Exception
  {
    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setCacheWindowDuration(2);
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.configurationSchema;
    @SuppressWarnings("unchecked")
    DimensionsQueueManager dqm = new DimensionsQueueManager(store, store.schemaRegistry, new SimpleDataQueryDimensionalExpander((Map) store.seenEnumValues));

    Map<String, Set<String>> fieldToAggregator = Maps.newHashMap();
    fieldToAggregator.put("impressions", Sets.newHashSet("SUM"));
    fieldToAggregator.put("cost", Sets.newHashSet("SUM"));

    FieldsAggregatable fieldsAggregatable = new FieldsAggregatable(fieldToAggregator);

    GPOMutable key = AppDataSingleSchemaDimensionStoreHDHTTest.createQueryKey(eventSchema,
                                                                              "google",
                                                                              "safeway");
    Map<String, Set<Object>> keyToValues = Maps.newHashMap();
    keyToValues.put("publisher", Sets.newHashSet());
    keyToValues.put("advertiser", Sets.newHashSet());

    DataQueryDimensional dqd = new DataQueryDimensional("1",
                                                        DataQueryDimensional.TYPE,
                                                        1,
                                                        new CustomTimeBucket(TimeBucket.MINUTE),
                                                        key.getFieldDescriptor(),
                                                        keyToValues,
                                                        fieldsAggregatable,
                                                        true);

    dqm.enqueue(dqd, null, null);

    Assert.assertEquals(9, store.getQueries().size());
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueueManagerTest.class);
}
