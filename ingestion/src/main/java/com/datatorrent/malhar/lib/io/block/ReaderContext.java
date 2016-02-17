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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PositionedReadable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This controls how an {@link AbstractBlockReader} reads a {@link BlockMetadata}.
 *
 * @param <STREAM> type of stream
 * @since 2.1.0
 */
public interface ReaderContext<STREAM extends InputStream & PositionedReadable>
{

  /**
   * Initializes the reader context.
   *
   * @param stream           input stream
   * @param blockMetadata    block-metadata
   * @param consecutiveBlock if current block was a consecutive block in the source. if it is then we continue reading
   *                         from the last offset.
   */
  void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock);

  /**
   * Reads an entity. Returns null when the block is processed.
   *
   * @return {@link Entity}. null when the work is done.
   * @throws IOException
   */
  Entity next() throws IOException;

  /**
   * Represents the total bytes used to construct the record.<br/>
   * Used bytes can be different from the bytes in the record.
   */
  public static class Entity
  {
    private byte[] record;
    private long usedBytes;

    public void clear()
    {
      record = null;
      usedBytes = -1;
    }

    public byte[] getRecord()
    {
      return record;
    }

    public void setRecord(byte[] record)
    {
      this.record = record;
    }

    public long getUsedBytes()
    {
      return usedBytes;
    }

    public void setUsedBytes(long usedBytes)
    {
      this.usedBytes = usedBytes;
    }
  }

  /**
   * An Abstract reader context which assumes that a block boundary is never crossed if the last entity read is fully
   * contained in the block.
   *
   * @param <STREAM> type of stream.
   */
  public static abstract class AbstractReaderContext<STREAM extends InputStream & PositionedReadable> implements ReaderContext<STREAM>
  {

    protected transient long offset;
    protected transient STREAM stream;
    protected transient BlockMetadata blockMetadata;

    protected final transient Entity entity;

    protected AbstractReaderContext()
    {
      offset = -1;
      entity = new Entity();
    }

    @Override
    public void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      this.stream = stream;
      this.blockMetadata = blockMetadata;
      if (!consecutiveBlock) {
        offset = blockMetadata.getOffset();
      }
    }

    @Override
    public Entity next() throws IOException
    {
      if (offset < blockMetadata.getLength()) {
        Entity entity = readEntity();
        offset += entity.usedBytes;
        return entity;
      }
      return null;
    }

    protected abstract Entity readEntity() throws IOException;
  }

  /**
   * This reader context splits the block into entities on '\n' or '\r'.<br/>
   * It will not read ahead of the block boundary if the last entity was completely contained in the block.<br/>
   * Any records formed using this context will need a way to validate the start of the record.
   *
   * @param <STREAM> type of stream.
   */

  public static class LineReaderContext<STREAM extends InputStream & PositionedReadable> extends AbstractReaderContext<STREAM>
  {

    protected int bufferSize;

    private final transient ByteArrayOutputStream lineBuilder;
    private final transient ByteArrayOutputStream emptyBuilder;
    private final transient ByteArrayOutputStream tmpBuilder;

    private transient byte[] buffer;
    private transient String strBuffer;
    private transient int posInStr;

    public LineReaderContext()
    {
      super();
      bufferSize = 8192;
      lineBuilder = new ByteArrayOutputStream();
      emptyBuilder = new ByteArrayOutputStream();
      tmpBuilder = new ByteArrayOutputStream();
    }

    @Override
    public void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      if (buffer == null) {
        buffer = new byte[bufferSize];
      }
      super.initialize(stream, blockMetadata, consecutiveBlock);
    }

    @Override
    protected Entity readEntity() throws IOException
    {
      //Implemented a buffered reader instead of using java's BufferedReader because it was reading much ahead of block boundary
      //and faced issues with duplicate records. Controlling the buffer size didn't help either.

      boolean foundEOL = false;
      int bytesRead = 0;
      long usedBytes = 0;

      while (!foundEOL) {
        tmpBuilder.reset();
        if (posInStr == 0) {
          bytesRead = stream.read(offset + usedBytes, buffer, 0, bufferSize);
          if (bytesRead == -1) {
            break;
          }
          strBuffer = new String(buffer);
        }

        while (posInStr < strBuffer.length()) {
          char c = strBuffer.charAt(posInStr);
          if (c != '\r' && c != '\n') {
            tmpBuilder.write(c);
            posInStr++;
          }
          else {
            foundEOL = true;
            break;
          }
        }
        byte[] subLine = tmpBuilder.toByteArray();
        usedBytes += subLine.length;
        lineBuilder.write(subLine);

        if (foundEOL) {
          while (posInStr < strBuffer.length()) {
            char c = strBuffer.charAt(posInStr);
            if (c == '\r' || c == '\n') {
              emptyBuilder.write(c);
              posInStr++;
            }
            else {
              break;
            }
          }
          usedBytes += emptyBuilder.toByteArray().length;
        }
        else {
          //read more bytes from the input stream
          posInStr = 0;
        }
      }
      posInStr = 0;
      //when end of stream is reached then bytesRead is -1
      if (bytesRead == -1) {
        lineBuilder.reset();
        emptyBuilder.reset();
        return null;
      }
      entity.clear();
      entity.record = lineBuilder.toByteArray();
      entity.usedBytes = usedBytes;

      lineBuilder.reset();
      emptyBuilder.reset();
      return entity;
    }

    /**
     * Sets the buffer size of read.
     *
     * @param bufferSize size of the buffer
     */
    public void setBufferSize(int bufferSize)
    {
      this.bufferSize = bufferSize;
    }

    /**
     * @return the buffer size of read.
     */
    public int getBufferSize()
    {
      return this.bufferSize;
    }
  }

  /**
   * Another reader context that splits the block into records on '\n' or '\r'.<br/>
   * This implementation doesn't need a way to validate the start of a record.<br/>
   *
   * This  starts parsing the block (except the first block of the file) from the first eol character.
   * It is a less optimized version of an {@link LineReaderContext} which always reads beyond the block
   * boundary.
   *
   * @param <STREAM>
   */
  public static class ReadAheadLineReaderContext<STREAM extends InputStream & PositionedReadable> extends LineReaderContext<STREAM>
  {
    @Override
    public void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      super.initialize(stream, blockMetadata, consecutiveBlock);
      //ignore first entity of  all the blocks except the first one because those bytes
      //were used during the parsing of the previous block.
      if (!consecutiveBlock && blockMetadata.getOffset() != 0) {
        try {
          Entity entity = readEntity();
          offset += entity.usedBytes;
        }
        catch (IOException e) {
          throw new RuntimeException("when reading first entity", e);
        }
      }
    }

    @Override
    public Entity next() throws IOException
    {
      if (offset < blockMetadata.getLength() || (offset == blockMetadata.getLength() && !blockMetadata.isLastBlock())) {
        Entity entity = readEntity();
        offset += entity.usedBytes;
        return entity;
      }
      return null;
    }
  }

  /**
   * This creates fixed sized entities.<br/>
   * It doesn't read beyond the block boundary therefore the last byte-array could be smaller.<br/>
   *
   * @param <STREAM> type of stream.
   */
  public static class FixedBytesReaderContext<STREAM extends InputStream & PositionedReadable> extends AbstractReaderContext<STREAM>
  {
    //When this field is null, it is initialized to default fs block size in setup.
    protected Integer length;

    @Override
    public void initialize(STREAM stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      if (length == null) {
        length = (int) new Configuration().getLong("fs.local.block.size", 32 * 1024 * 1024);
        LOG.debug("length init {}", length);
      }
      super.initialize(stream, blockMetadata, consecutiveBlock);
    }

    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      int bytesToRead = length;
      if (offset + length >= blockMetadata.getLength()) {
        bytesToRead = (int) (blockMetadata.getLength() - offset);
      }
      byte[] record = new byte[bytesToRead];
      stream.readFully(offset, record, 0, bytesToRead);
      entity.usedBytes = bytesToRead;
      entity.record = record;

      return entity;
    }

    /**
     * Sets the length of each record.
     *
     * @param length fixed length of each record.
     */
    public void setLength(Integer length)
    {
      this.length = length;
    }

    /**
     * @return the length of record.
     */
    public Integer getLength()
    {
      return this.length;
    }

    private static final Logger LOG = LoggerFactory.getLogger(FixedBytesReaderContext.class);
  }
}
