/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.module.io.fs;

import com.datatorrent.lib.io.block.BlockMetadata.AbstractBlockMetadata;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;



  /**
   * A block of file which contains file path adn other block properties.
   */
  public class ExtendedFileBlockMetadata extends FileBlockMetadata
  {
    private long compressionTime;
    private long compressedSize; 

    protected ExtendedFileBlockMetadata()
    {
      super();
    }

    public ExtendedFileBlockMetadata(String filePath, long blockId, long offset, long length, boolean isLastBlock, long previousBlockId)
    {
      super(filePath, blockId, offset, length, isLastBlock, previousBlockId);
    }
    
    protected ExtendedFileBlockMetadata(FileBlockMetadata fileBlockMetadata)
    {
      super(fileBlockMetadata.getFilePath(), fileBlockMetadata.getBlockId(), fileBlockMetadata.getOffset(), 
          fileBlockMetadata.getLength(), fileBlockMetadata.isLastBlock(), fileBlockMetadata.getPreviousBlockId());
    }

    
    public long getCompressionTime()
    {
      return compressionTime;
    }

    public void setCompressionTime(long compressionTime)
    {
      this.compressionTime = compressionTime;
    }

    public long getCompressedSize()
    {
      return compressedSize;
    }

    public void setCompressedSize(long compressionSize)
    {
      this.compressedSize = compressionSize;
    }
  }

