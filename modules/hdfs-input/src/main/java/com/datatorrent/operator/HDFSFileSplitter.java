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

package com.datatorrent.operator;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.input.ModuleFileSplitter;

public class HDFSFileSplitter extends ModuleFileSplitter
{
  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileSplitter.class);
  private transient FileSystem appFS;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    Scanner scanner = (Scanner)getScanner();
    if (scanner.getIgnoreFilePatternRegularExp() == null) {
      scanner.setIgnoreFilePatternRegularExp(".*._COPYING_");
    }
  }

  private FileSystem getLocalFS() throws IOException
  {
    return FileSystem.newInstance(new Configuration());
  }

  @Override
  protected long getDefaultBlockSize()
  {
    try {
      if (appFS == null) {
        appFS = getLocalFS();
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to get FileSystem instance.", e);
    }
    //getting block size of local HDFS by default, to optimize the blocks for fast merge
    return appFS.getDefaultBlockSize(new Path(context.getValue(DAGContext.APPLICATION_PATH)));
  }

  @Override
  public void teardown()
  {
    super.teardown();
    if (appFS != null) {
      try {
        appFS.close();
      } catch (IOException e) {
        throw new RuntimeException("Unable to close application file system.", e);
      }
    }
  }

  public static class HDFSScanner extends Scanner
  {
    @Override
    protected boolean acceptFile(String filePathStr)
    {
      boolean accepted = super.acceptFile(filePathStr);
      if (containsUnsupportedCharacters(filePathStr)) {
        return false;
      }
      return accepted;
    }

    private boolean containsUnsupportedCharacters(String filePathStr)
    {
      return new Path(filePathStr).toUri().getPath().contains(":");
    }
  }
}
