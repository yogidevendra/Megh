/*
 *  Copyright (c) 2016 DataTorrent, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io.output;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * EncryptionMetaData class.
 * </p>
 *
 */
public class EncryptionMetaData implements Serializable
{
  private static final long serialVersionUID = 5453280767498532596L;
  public static final String KEY = "key";
  public static final String TRANSFORMATION = "transformation";
  private final Map<String, Object> metadata = new HashMap<String, Object>();

  public void setKey(byte[] key)
  {
    metadata.put(KEY, key);
  }

  public void setTransformation(String transformation)
  {
    metadata.put(TRANSFORMATION, transformation);
  }

  public Map<String, Object> getMetadata()
  {
    return metadata;
  }
}
