/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;

/**
 * This class contains utility methods which are useful for aggregators.
 *
 * @since 3.1.0
 */
public final class AggregatorUtils
{
  /**
   * This is an identity type map, which maps input types to the same output types.
   */
  public static final transient Map<Type, Type> IDENTITY_TYPE_MAP;
  /**
   * This is an identity type map, for numeric types only. This is
   * helpful when creating aggregators like {@link AggregatorSum}, where the sum of ints is an
   * int and the sum of floats is a float.
   */
  public static final transient Map<Type, Type> IDENTITY_NUMBER_TYPE_MAP;

  static {
    Map<Type, Type> identityTypeMap = Maps.newHashMap();

    for (Type type : Type.values()) {
      identityTypeMap.put(type, type);
    }

    IDENTITY_TYPE_MAP = Collections.unmodifiableMap(identityTypeMap);

    Map<Type, Type> identityNumberTypeMap = Maps.newHashMap();

    for (Type type : Type.NUMERIC_TYPES) {
      identityNumberTypeMap.put(type, type);
    }

    IDENTITY_NUMBER_TYPE_MAP = Collections.unmodifiableMap(identityNumberTypeMap);
  }

  /**
   * Don't instantiate this class.
   */
  private AggregatorUtils()
  {
    //Don't instantiate this class.
  }

  /**
   * This is a helper method which takes a {@link FieldsDescriptor} object, which defines the types of the fields
   * that the {@link IncrementalAggregator} receives as input. It then uses the given {@link IncrementalAggregator}
   * and {@link FieldsDescriptor} object to compute the {@link FieldsDescriptor} object for the aggregation produced
   * byte the given
   * {@link IncrementalAggregator} when it receives an input corresponding to the given input {@link FieldsDescriptor}.
   *
   * @param inputFieldsDescriptor This is a {@link FieldsDescriptor} object which defines the names and types of input
   *                              data recieved by an aggregator.
   * @param incrementalAggregator This is the
   * {@link IncrementalAggregator} for which an output {@link FieldsDescriptor} needs
   *                              to be computed.
   * @return The output {@link FieldsDescriptor} for this aggregator when it receives input data with the same schema as
   * the specified input {@link FieldsDescriptor}.
   */
  public static FieldsDescriptor getOutputFieldsDescriptor(FieldsDescriptor inputFieldsDescriptor,
      IncrementalAggregator incrementalAggregator)
  {
    Map<String, Type> fieldToType = Maps.newHashMap();

    for (Map.Entry<String, Type> entry :
        inputFieldsDescriptor.getFieldToType().entrySet()) {
      String fieldName = entry.getKey();
      Type fieldType = entry.getValue();
      Type outputType = incrementalAggregator.getOutputType(fieldType);
      fieldToType.put(fieldName, outputType);
    }

    return new FieldsDescriptor(fieldToType);
  }

  /**
   * This is a utility method which creates an output {@link FieldsDescriptor} using the field names
   * from the given {@link FieldsDescriptor} and the output type of the given {@link OTFAggregator}.
   *
   * @param inputFieldsDescriptor The {@link FieldsDescriptor} from which to derive the field names used
   *                              for the output fields descriptor.
   * @param otfAggregator         The {@link OTFAggregator} to use for creating the output {@link FieldsDescriptor}.
   * @return The output {@link FieldsDescriptor}.
   */
  public static FieldsDescriptor getOutputFieldsDescriptor(FieldsDescriptor inputFieldsDescriptor,
      OTFAggregator otfAggregator)
  {
    Map<String, Type> fieldToType = Maps.newHashMap();

    for (Map.Entry<String, Type> entry :
        inputFieldsDescriptor.getFieldToType().entrySet()) {
      String fieldName = entry.getKey();
      Type outputType = otfAggregator.getOutputType();
      fieldToType.put(fieldName, outputType);
    }

    return new FieldsDescriptor(fieldToType);
  }

  /**
   * This is a utility method which creates an output {@link FieldsDescriptor} from the
   * given field names and the given {@link OTFAggregator}.
   *
   * @param fields        The names of the fields to be included in the output {@link FieldsDescriptor}.
   * @param otfAggregator The {@link OTFAggregator} to use when creating the output {@link FieldsDescriptor}.
   * @return The output {@link FieldsDescriptor}.
   */
  public static FieldsDescriptor getOutputFieldsDescriptor(Fields fields,
      OTFAggregator otfAggregator)
  {
    Map<String, Type> fieldToType = Maps.newHashMap();

    for (String field : fields.getFields()) {
      fieldToType.put(field, otfAggregator.getOutputType());
    }

    return new FieldsDescriptor(fieldToType);
  }
}
