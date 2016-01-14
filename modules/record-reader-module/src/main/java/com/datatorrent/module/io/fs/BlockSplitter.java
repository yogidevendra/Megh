/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module.io.fs;

import com.datatorrent.common.util.BaseOperator;

/**
 * This class is responsible for splitting block data into records. Records
 * crossing the block boundary are passed on to
 * {@link BlockBoundarySynchronizer}
 *
 * @param <T>
 */
class BlockSplitter<T> extends BaseOperator
{

}
