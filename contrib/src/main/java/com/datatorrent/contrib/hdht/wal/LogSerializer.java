/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht.wal;

import com.datatorrent.netlet.util.Slice;

/**
 *
 * @since 3.3.0
 */

public interface LogSerializer<T>
{
  Slice fromObject(T entry);

  T toObject(Slice s);
}
