/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.module.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * This interface defines responsibilities for record scanner. Record scanner is
 * used to extract valid records from given byte[] based on pre-define record
 * definition
 */
public interface RecordScanner
{
  /**
   * Extract valid records from input block data based on pre-defined record
   * definition.
   * 
   * @param input
   *          block data
   * @return BlockConstituentRecords
   */
  public BlockConstituentRecords split(byte[] input);

  /**
   * This class holds block data as separated into individual records after
   * split operation
   */
  public static class BlockConstituentRecords
  {
    /**
     * This represents chunk of data at the begining of the block. This chunk of
     * data may or maynot represent valid record. Sometimes, part of the data for
     * this record could be present in the earlier blocks. In this case, this
     * should be merged with appropriate blocks to stitch complete record.
     */
    byte[] blockBeginPartialRecord = EMPTY_BYTE_ARRAY;

    /**
     * This represents chunk of data at the end of the block. This chunk of data
     * may or maynot represent valid record. Sometimes, part of the data for this
     * record could be present in the subsequent blocks. In this case, this should
     * be merged with appropriate blocks to stitch complete record.
     */
    byte[] blockEndPartialRecord = EMPTY_BYTE_ARRAY;

    /**
     * List of records that are completely spanned inside given byte[] block
     */
    List<byte[]> blockSpannedRecords = EMTPY_BLOCK_RECORD_LIST;
    
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final List<byte[]> EMTPY_BLOCK_RECORD_LIST = new ArrayList<byte[]>(0);
    
  }

}
