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

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.input.ModuleFileSplitter;

/**
 * HDFSFileSplitter extends {@link ModuleFileSplitter} to: <br/>
 * 1. Ignore hadoop temporary files i.e. files with extension _COPYING_ <br/>
 * 2. Ignore files with unsupported characters in filepath
 */
public class HDFSFileSplitter extends ModuleFileSplitter
{
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    Scanner scanner = (Scanner)getScanner();
    if (scanner.getIgnoreFilePatternRegularExp() == null) {
      scanner.setIgnoreFilePatternRegularExp(".*._COPYING_");
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
