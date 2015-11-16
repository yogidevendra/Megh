/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.io.block;

public class ModuleBlockMetadata extends BlockMetadata.FileBlockMetadata
{
  boolean readBlockInSequence;

  protected ModuleBlockMetadata()
  {
    super();
  }

  public ModuleBlockMetadata(String filePath, long blockId, long offset, long length, boolean isLastBlock,
      long previousBlockId)
  {
    super(filePath, blockId, offset, length, isLastBlock, previousBlockId);
  }

  public ModuleBlockMetadata(String filePath)
  {
    super(filePath);
  }

  @Override
  public int hashCode()
  {
    if (isReadBlockInSequence()) {
      return getFilePath().hashCode();
    }
    return super.hashCode();
  }

  public boolean isReadBlockInSequence()
  {
    return readBlockInSequence;
  }

  public void setReadBlockInSequence(boolean readBlockInSequence)
  {
    this.readBlockInSequence = readBlockInSequence;
  }

}
