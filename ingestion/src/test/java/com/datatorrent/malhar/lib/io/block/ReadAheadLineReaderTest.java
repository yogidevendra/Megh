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
package com.datatorrent.malhar.lib.io.block;

public class ReadAheadLineReaderTest extends FSLineReaderTest
{
  @Override
  AbstractFSBlockReader<String> getBlockReader()
  {
    return new ReadAheadBlockReader();
  }

  public static final class ReadAheadBlockReader extends AbstractFSBlockReader.AbstractFSReadAheadLineReader<String>
  {
    @Override
    protected String convertToRecord(byte[] bytes)
    {
      return new String(bytes);
    }
  }
}