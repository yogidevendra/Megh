/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.contrib.enrichment;

import java.util.List;

import com.datatorrent.lib.db.cache.CacheManager;
/**
 * @since 3.1.0
 */

public interface EnrichmentBackup extends CacheManager.Backup
{
  public void setFields(List<String> lookupFields,List<String> includeFields);

  public boolean needRefresh();
}
