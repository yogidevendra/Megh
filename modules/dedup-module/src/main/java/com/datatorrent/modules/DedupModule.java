package com.datatorrent.modules;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.lib.bucket.BasicBucketManagerPOJOImpl;
import com.datatorrent.lib.bucket.BucketStore;
import com.datatorrent.lib.bucket.ExpirableCategoricalBucketManagerPOJOImpl;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.ExpirableOrderedBucketManagerPOJOImpl;
import com.datatorrent.lib.bucket.ExpirableTimeBasedBucketManagerPOJOImpl;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.dedup.DeduperCategoricalPOJOImpl;
import com.datatorrent.lib.dedup.DeduperOptimizedPOJOImpl;
import com.datatorrent.lib.dedup.DeduperOrderedPOJOImpl;
import com.datatorrent.lib.dedup.DeduperTimeBasedPOJOImpl;

/**
 * Implementation of {@link #DedupModule()}. Dag for this module consists of just a single operator - Deduper
 *
 * Input Ports {@link #input} - input tuples of type pojo
 *
 * Output Ports {@link #unique} - Unique port (pojo) {@link #duplicate} - Duplicate port (pojo) {@link #expired} -
 * Expired port (pojo) {@link #error} - Error port (pojo)
 *
 */
public class DedupModule implements Module
{
  /**
   * Input port for Dedup module - Accepts POJO tuples
   */
  public final transient ProxyInputPort<Object> input = new ProxyInputPort<Object>();

  /**
   * Unique output port for Dedup module
   */
  public final transient ProxyOutputPort<Object> unique = new ProxyOutputPort<Object>();

  /**
   * Duplicate output port for Dedup module
   */
  public final transient ProxyOutputPort<Object> duplicate = new ProxyOutputPort<Object>();

  /**
   * Expired output port for Dedup module
   */
  public final transient ProxyOutputPort<Object> expired = new ProxyOutputPort<Object>();

  /**
   * Expired output port for Dedup module
   */
  public final transient ProxyOutputPort<Object> error = new ProxyOutputPort<Object>();

  /**
   * tupleClass - The qualified class name for the tuple objects
   */
  @NotNull
  String tupleClass = "com.datatorrent.modules.utils.POJO";

  /**
   * dedupKey - Key in tuple based on which de-duplication happens
   */
  String dedupKey;

  /**
   * isExpiry - Whether incoming tuples can be expired
   */
  boolean isExpiry = false; // default

  /**
   * expiryType - The type of expiry. Possible values are: ORDERED, TUPLETIME, SYSTEMTIME, CATEGORICAL
   */
  String expiryType;

  /**
   * expiryKey - The key in tuple used for expiry
   */
  String expiryKey;

  /**
   * expiryPeriod - The period used for expiring incoming tuples. This depends on the type of expiry. Following is the
   * interpretation in case of various expiry cases: Ordered Expiry - Value in the domain of the Time Based Expiry -
   * Value in seconds Categorical Expiry - Value in number of categories
   */
  long expiryPeriod;

  /**
   * numberOfBuckets - The number of buckets used to store the incoming key space For non-expiry cases, this has to be
   * configured. This indicates the number of buckets the entire key space is classified into. However, for expiry
   * cases, this will be computed as {@link #numberOfBuckets} = {@link #expiryPeriod}/{@link #bucketSpan}.
   */
  int numberOfBuckets;

  /**
   * bucketSpan - The span of a single bucket For non-expiry cases, this does not have to be configured. It is
   * automatically set to the hash of {@link #dedupKey}/{@link #numberOfBuckets} For expiry cases, this indicates the
   * width of the bucket. In this case, {@link #numberOfBuckets} = {@link #expiryPeriod}/{@link #bucketSpan}
   */
  long bucketSpan;

  /**
   * maxNumberOfBucketsInMemory - This indicates the maximum number of buckets that can stay in memory. This has to be
   * carefully set for the module as this may impact the amount of data being held in memory. Too high a value may
   * result in memory overflow errors, while too low may result in degraded performance.
   */
  int numBucketsInMemory;

  /**
   * useSystemTime - In case of time based expiry key, whether to use System time as the reference or the tuple time
   */
  boolean useSystemTime = true; // default

  /**
   * maxExpiryJump - In case of ordered/tuple time based expiry key, how much further the incoming tuple can take the
   * reference time ahead Taking this time too ahead may result in all of the existing tuples being expired.
   */
  long maxExpiryJump = 60 * 60 * 3; // default - 3 hours

//  /**
//   * writeDataAtCheckpoint - Boolean optimization parameter which indicates whether to write bucket data to persistent
//   * storage only at checkpoints or every window.
//   */
//  boolean persistBucketsAtCheckpoints = true; // default

  /**
   * persistEntireBucket - Boolean optimization parameter which indicates whether to write entire bucket data (dirty +
   * already persisted) into the persistent storage.
   */
  boolean persistEntireBucket = false; // default

  /**
   * maintainInputOrder - Boolean parameter deciding whether to emit the tuples in the order in which they were received
   * on the {@link #input} port.
   */
  boolean maintainInputOrder = true; // default

  /**
   * enableBloomFilter - Boolean parameter indicating whether to use Bloom filter for identifying unique tuples. This is
   * an optimization parameter. Defaults for {@link #maxBloomExpectedElements} and {@link #bloomFalsePositiveProb} are
   * set such that the size of a bloom filter stays ~ 10KB
   */
  boolean enableBloomFilter = true; // default

  /**
   * maxExpectedTuples - Maximum number of elements expected to be inserted into the bloom filter.
   */
  int maxBloomExpectedElements = 10000; // default

  /**
   * falsePositiveProbability - Probability of false positives (returning that element is duplicate when in fact it is
   * unique) by the bloom filter
   */
  double bloomFalsePositiveProb = 0.01; // default

  /**
   * partitionCount - Static partition count for Dedup Module. Determines the number of physical partitions for the
   * module.
   */
  int staticPartitionCount = 1; // default

  /**
   * populateDag() for Dedup Module Dag just consists of a single operator - Deduper Depending on the presence and type
   * of expiry, various parameters need to be set on the Deduper
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Class<?> schemaClass = null;
    if (!isExpiry) {
      // Add operator to Dag
      DeduperOptimizedPOJOImpl deduper = dag.addOperator("Deduper", new DeduperOptimizedPOJOImpl());
      // deduper.setSaveDataAtCheckpoint(persistBucketsAtCheckpoints);
      deduper.setOrderedOutput(maintainInputOrder);
      deduper.setUseBloomFilter(enableBloomFilter);
      if (enableBloomFilter) {
        deduper.setExpectedNumTuples(maxBloomExpectedElements);
        deduper.setFalsePositiveProb(bloomFalsePositiveProb);
      }
//      schemaClass = StramUtils.classForName(tupleClass, Object.class);
//      dag.getMeta(deduper).getMeta(deduper.input).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);
      // Set Ports
      input.set(deduper.input);
      unique.set(deduper.output);
      duplicate.set(deduper.duplicates);
      expired.set(deduper.expired);
      error.set(deduper.error);

      BasicBucketManagerPOJOImpl bucketManager = new BasicBucketManagerPOJOImpl();
      bucketManager.setKeyExpression(dedupKey);
      bucketManager.setNoOfBuckets(numberOfBuckets);
      bucketManager.setNoOfBucketsInMemory(numBucketsInMemory);
      bucketManager.setMaxNoOfBucketsInMemory(numBucketsInMemory);
      bucketManager.setCollateFilesForBucket(persistEntireBucket);
      BucketStore<Object> bucketStore = new HdfsBucketStore<Object>();
      bucketManager.setBucketStore(bucketStore);
      deduper.setBucketManager(bucketManager);
    } else {
      ExpiryType expiry = ExpiryType.valueOf(expiryType);
      switch (expiry) {
        case ORDERED:
          // Add operator to Dag
          DeduperOrderedPOJOImpl orderedDeduper = dag.addOperator("Deduper", new DeduperOrderedPOJOImpl());
          // deduper.setSaveDataAtCheckpoint(persistBucketsAtCheckpoints);
          orderedDeduper.setOrderedOutput(maintainInputOrder);
          orderedDeduper.setUseBloomFilter(enableBloomFilter);
          if (enableBloomFilter) {
            orderedDeduper.setExpectedNumTuples(maxBloomExpectedElements);
            orderedDeduper.setFalsePositiveProb(bloomFalsePositiveProb);
          }
//          schemaClass = StramUtils.classForName(tupleClass, Object.class);
//          dag.getMeta(orderedDeduper).getMeta(orderedDeduper.input).getAttributes()
//              .put(Context.PortContext.TUPLE_CLASS, schemaClass);
          // Set Ports
          input.set(orderedDeduper.input);
          unique.set(orderedDeduper.output);
          duplicate.set(orderedDeduper.duplicates);
          expired.set(orderedDeduper.expired);
          error.set(orderedDeduper.error);

          ExpirableOrderedBucketManagerPOJOImpl bucketManager = new ExpirableOrderedBucketManagerPOJOImpl();
          bucketManager.setKeyExpression(dedupKey);
          bucketManager.setExpiryExpression(expiryKey);
          bucketManager.setExpiryPeriod(expiryPeriod);
          bucketManager.setBucketSpan(bucketSpan);
          bucketManager.setMaxExpiryJump(maxExpiryJump);
          // bucketManager.setNoOfBuckets(numberOfBuckets);
          // bucketManager.setNoOfBucketsInMemory(numBucketsInMemory);
          // bucketManager.setMaxNoOfBucketsInMemory(numBucketsInMemory);
          bucketManager.setCollateFilesForBucket(persistEntireBucket);
          BucketStore<Object> bucketStore = new ExpirableHdfsBucketStore<Object>();
          bucketManager.setBucketStore(bucketStore);
          orderedDeduper.setBucketManager(bucketManager);
          break;
        case TUPLETIME:
        case SYSTEMTIME:
          // Add operator to Dag
          DeduperTimeBasedPOJOImpl timeBasedDeduper = dag.addOperator("Deduper", new DeduperTimeBasedPOJOImpl());
          // timeBasedDeduper.setSaveDataAtCheckpoint(persistBucketsAtCheckpoints);
          timeBasedDeduper.setOrderedOutput(maintainInputOrder);
          timeBasedDeduper.setUseBloomFilter(enableBloomFilter);
          if (enableBloomFilter) {
            timeBasedDeduper.setExpectedNumTuples(maxBloomExpectedElements);
            timeBasedDeduper.setFalsePositiveProb(bloomFalsePositiveProb);
          }
//          schemaClass = StramUtils.classForName(tupleClass, Object.class);
//          dag.getMeta(timeBasedDeduper).getMeta(timeBasedDeduper.input).getAttributes()
//              .put(Context.PortContext.TUPLE_CLASS, schemaClass);
          // Set Ports
          input.set(timeBasedDeduper.input);
          unique.set(timeBasedDeduper.output);
          duplicate.set(timeBasedDeduper.duplicates);
          expired.set(timeBasedDeduper.expired);
          error.set(timeBasedDeduper.error);

          ExpirableTimeBasedBucketManagerPOJOImpl timeBasedBucketManager =
              new ExpirableTimeBasedBucketManagerPOJOImpl();
          timeBasedBucketManager.setKeyExpression(dedupKey);
          timeBasedBucketManager.setTimeExpression(expiryKey);
          timeBasedBucketManager.setExpiryPeriod(expiryPeriod);
          timeBasedBucketManager.setBucketSpan(bucketSpan);
          timeBasedBucketManager.setUseSystemTime(useSystemTime);
          timeBasedBucketManager.setMaxExpiryJump(maxExpiryJump);
          // timeBasedBucketManager.setNoOfBuckets(numberOfBuckets);
          // timeBasedBucketManager.setNoOfBucketsInMemory(numBucketsInMemory);
          // timeBasedBucketManager.setMaxNoOfBucketsInMemory(numBucketsInMemory);
          timeBasedBucketManager.setCollateFilesForBucket(persistEntireBucket);
          BucketStore<Object> tupleTimeBucketStore = new ExpirableHdfsBucketStore<Object>();
          timeBasedBucketManager.setBucketStore(tupleTimeBucketStore);
          timeBasedDeduper.setBucketManager(timeBasedBucketManager);
          break;
        case CATEGORICAL:
          // Add operator to Dag
          DeduperCategoricalPOJOImpl categoricalDeduper = dag.addOperator("Deduper", new DeduperCategoricalPOJOImpl());
          // timeBasedDeduper.setSaveDataAtCheckpoint(persistBucketsAtCheckpoints);
          categoricalDeduper.setOrderedOutput(maintainInputOrder);
          categoricalDeduper.setUseBloomFilter(enableBloomFilter);
          if (enableBloomFilter) {
            categoricalDeduper.setExpectedNumTuples(maxBloomExpectedElements);
            categoricalDeduper.setFalsePositiveProb(bloomFalsePositiveProb);
          }
//          schemaClass = StramUtils.classForName(tupleClass, Object.class);
//          dag.getMeta(categoricalDeduper).getMeta(categoricalDeduper.input).getAttributes()
//              .put(Context.PortContext.TUPLE_CLASS, schemaClass);
          // Set Ports
          input.set(categoricalDeduper.input);
          unique.set(categoricalDeduper.output);
          duplicate.set(categoricalDeduper.duplicates);
          expired.set(categoricalDeduper.expired);
          error.set(categoricalDeduper.error);

          ExpirableCategoricalBucketManagerPOJOImpl categoricalBucketManager =
              new ExpirableCategoricalBucketManagerPOJOImpl();
          categoricalBucketManager.setKeyExpression(dedupKey);
          categoricalBucketManager.setExpiryExpression(expiryKey);
          categoricalBucketManager.setExpiryBuckets((int)expiryPeriod);
          categoricalBucketManager.setNoOfBuckets(numberOfBuckets);
          categoricalBucketManager.setNoOfBucketsInMemory(numBucketsInMemory);
          categoricalBucketManager.setMaxNoOfBucketsInMemory(numBucketsInMemory);
          categoricalBucketManager.setCollateFilesForBucket(persistEntireBucket);
          BucketStore<Object> categoricalBucketStore = new ExpirableHdfsBucketStore<Object>();
          categoricalBucketManager.setBucketStore(categoricalBucketStore);
          categoricalDeduper.setBucketManager(categoricalBucketManager);

          break;
        default:
          throw new RuntimeException("Expiry Type " + expiryType + " unknown");
      }
    }
  }

  protected enum ExpiryType
  {
    ORDERED, TUPLETIME, SYSTEMTIME, CATEGORICAL
  }

  /*
   * Setters for module properties
   */

  /**
   * The primary key for the deduper. The de-duplication will be based on this key.
   *
   * @param dedupKey
   */
  public void setDedupKey(String dedupKey)
  {
    this.dedupKey = dedupKey;
  }

  /**
   * Indicates whether or not an expiry use case. If an expiry usecase, additional parameters need to be specified
   * including expiryKey, expityType and expiryPeriod.
   *
   * @param isExpiry
   */
  public void setIsExpiry(boolean isExpiry)
  {
    this.isExpiry = isExpiry;
  }

  /**
   * The type of expiry. Following are valid values for this attribute: 1. ORDERED 2. TUPLETIME 3. SYSTEMTIME 4.
   * CATEGORICAL
   *
   * @param expiryType
   */
  public void setExpiryType(String expiryType)
  {
    this.expiryType = expiryType;
  }

  /**
   * The expiry key for the deduper. The expiry of records/keys will be based on this key.
   *
   * @param expiryKey
   */
  public void setExpiryKey(String expiryKey)
  {
    this.expiryKey = expiryKey;
  }

  /**
   * Sets the period of expiry. Depending on the use case, the unit for the period may depend on the domain of expiry
   * key
   *
   * @param expiryPeriod
   */
  public void setExpiryPeriod(long expiryPeriod)
  {
    this.expiryPeriod = expiryPeriod;
  }

  /**
   * Sets the number of buckets into which the keys for tuples will be stored. This is closely related to the
   * {@link #bucketSpan} parameter.
   * 
   * @see #bucketSpan
   *
   * @param numberOfBuckets
   */
  public void setNumberOfBuckets(int numberOfBuckets)
  {
    this.numberOfBuckets = numberOfBuckets;
  }

  /**
   * Indicates the span of a single bucket. This in turn depends on the number of buckets. Increasing the
   * {@link #bucketSpan}, may decrease the {@link #numberOfBuckets} and vice-versa.
   * 
   * @see #numberOfBuckets
   *
   * @param bucketSpan
   */
  public void setBucketSpan(long bucketSpan)
  {
    this.bucketSpan = bucketSpan;
  }

  /**
   * This indicates the number of buckets that may stay in memory concurrently. If more buckets need to be stored in
   * memory, some may be flushed to disk and lazily loaded later when needed. This has a performance impact in that
   * increasing number of buckets may improve performance as less buckets will cause a cache miss.
   *
   * @param numBucketsInMemory
   */
  public void setNumBucketsInMemory(int numBucketsInMemory)
  {
    this.numBucketsInMemory = numBucketsInMemory;
  }

  /**
   * Indicates whether System time will be used to keep track of the time. If set to false, the time indicated by the
   * incoming tuple will be used as a reference instead.
   *
   * @param useSystemTime
   */
  public void setUseSystemTime(boolean useSystemTime)
  {
    this.useSystemTime = useSystemTime;
  }

  // public void setPersistBucketsAtCheckpoints(boolean persistBucketsAtCheckpoints)
  // {
  // this.persistBucketsAtCheckpoints = persistBucketsAtCheckpoints;
  // }

  /**
   * Sets how long a tuple can take the reference latest point ahead. Applicable to tuple time based and ordered expiry.
   * Note: Setting this too high may result in all of the existing tuples in the system to be deemed expired based on
   * the expiry period.
   * 
   * @param maxExpiryJump
   */
  public void setMaxExpiryJump(long maxExpiryJump)
  {
    this.maxExpiryJump = maxExpiryJump;
  }

  /**
   * Indicates whether an entire bucket must be persisted as opposed to only the unwritten data in the bucket. This may
   * have a performance impact in that a bucket which is spread over multiple different files may take a larger time to
   * get loaded as opposed to a bucket which is just present in a single file.
   *
   * @param persistEntireBucket
   */
  public void setPersistEntireBucket(boolean persistEntireBucket)
  {
    this.persistEntireBucket = persistEntireBucket;
  }

  /**
   * Whether or not the order of input tuples needs to be maintained. If set to false, tuples may be emitted out of
   * order depending on which gets processed first.
   *
   * @param maintainInputOrder
   */
  public void setMaintainInputOrder(boolean maintainInputOrder)
  {
    this.maintainInputOrder = maintainInputOrder;
  }

  /**
   * Whether to enable use of bloom filter in the de-duplication algorithm. Bloom filter may improve performance in case
   * not a lot of buckets are present in memory which causes cache misses frequently. Such cache misses may be avoided
   * by using bloom filter.
   *
   * @param enableBloomFilter
   */
  public void setEnableBloomFilter(boolean enableBloomFilter)
  {
    this.enableBloomFilter = enableBloomFilter;
  }

  /**
   * Bloom filter parameter, indicates the max number of elements per bloom filter to guarantee the performance
   * contract.
   *
   * @see #bloomFalsePositiveProb
   *
   * @param maxBloomExpectedElements
   */
  public void setMaxBloomExpectedElements(int maxBloomExpectedElements)
  {
    this.maxBloomExpectedElements = maxBloomExpectedElements;
  }

  /**
   * Sets the false positive probability for a bloom filter. This is the probability of false positive responses which
   * is guaranteed by the bloom filter.
   *
   * @see #maxBloomExpectedElements
   *
   * @param bloomFalsePositiveProb
   */
  public void setBloomFalsePositiveProb(double bloomFalsePositiveProb)
  {
    this.bloomFalsePositiveProb = bloomFalsePositiveProb;
  }

  /**
   * Sets the partition count for the De-duplication module. This indicates the number of instances of this module which
   * can work in parallel on the incoming data stream. Note that in expiry cases, the maintainence of the expiry point
   * will be restricted to just a single instance. In other words, results may vary if multiple instances of this module
   * are running, as opposed to just a single instance.
   *
   * @param partitionCount
   */
  public void setStaticPartitionCount(int partitionCount)
  {
    this.staticPartitionCount = partitionCount;
  }

  public void setTupleClass(String tupleClass)
  {
    this.tupleClass = tupleClass;
  }

}
