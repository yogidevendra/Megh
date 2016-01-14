/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.module.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * This class defines scanner for delimited records which can be possibly
 * variable length. Record boundaries are identified by some delimiter string.
 * It is assumed that delimiter string will not be present inside record data.
 */
public class DelimitedRecordScanner implements RecordScanner
{

  String delimiterString;

  @Override
  public BlockConstituentRecords split(byte[] input)
  {
    String inputString = new String(input);

    //Based on SplitterBenchmarkTest inputString.split performs better than Gauva.splitter
    String[] tokens = inputString.split(delimiterString);

    BlockConstituentRecords blockConstituentRecords = new BlockConstituentRecords();

    if (tokens.length > 0) {
      String beginToken = tokens[0];
      blockConstituentRecords.blockBeginPartialRecord = beginToken.getBytes();

      if (tokens.length > 1) {
        String endToken = tokens[tokens.length - 1];
        blockConstituentRecords.blockEndPartialRecord = endToken.getBytes();
      }

      if (tokens.length > 2) {
        List<byte[]> blockSpannedRecords = new ArrayList<byte[]>(tokens.length - 2);
        //Iterate over all tokens which are completely spanned in block (except first, last token)
        for (int i = 1; i < tokens.length - 1; i++) {
          blockSpannedRecords.add(tokens[i].getBytes());
        }
        blockConstituentRecords.blockSpannedRecords = blockSpannedRecords;
      }
    }
    
    return blockConstituentRecords;
  }

}
