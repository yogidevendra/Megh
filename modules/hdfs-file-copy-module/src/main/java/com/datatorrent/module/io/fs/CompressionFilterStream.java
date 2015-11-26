package com.datatorrent.module.io.fs;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.lang.mutable.MutableLong;

import com.datatorrent.lib.io.fs.FilterStreamContext;
import com.datatorrent.lib.io.fs.FilterStreamProvider;

/**
 * <p>CompressionFilterStream class.</p>
 *
 * @since 1.0.0
 */
public class CompressionFilterStream
{
  public static class TimedCompressionOutputStream extends FilterOutputStream
  {
    private MutableLong timeTakenNano;
    private long streamTimeNanos;
    private long streamSizeBytes;

    public TimedCompressionOutputStream(OutputStream out, MutableLong timeTakenNano)
    {
      super(out);
      this.timeTakenNano = timeTakenNano;
      this.streamTimeNanos = 0;
    }

    /**
     * Calls write on underlying FilterOutputStream. Records time taken in executing the write call.
     */
    @Override
    public synchronized void write(byte[] buffer, int off, int len) throws IOException
    {
      long startTime = System.nanoTime();
      super.write(buffer, off, len);
      long endTime = System.nanoTime();
      timeTakenNano.add(endTime - startTime);
      streamTimeNanos += (endTime - startTime);
      streamSizeBytes = len;
    }

    @Override
    public void flush() throws IOException
    {
      long startTime = System.nanoTime();
      super.flush();
      long endTime = System.nanoTime();
      streamTimeNanos += (endTime - startTime);
    }

    public long getStreamTimeNanos()
    {
      return streamTimeNanos;
    }

    public long getStreamSizeBytes()
    {
      return streamSizeBytes;
    }
  }

  public static class CompressionFiltertreamContext extends FilterStreamContext.BaseFilterStreamContext<TimedCompressionOutputStream>
  {
    public CompressionFiltertreamContext(String compressionClassName, OutputStream outputStream, MutableLong timeTakenNano) throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException
    {
      Constructor c = Class.forName(compressionClassName).getConstructor(OutputStream.class);
      FilterOutputStream lzoOutputStream = (FilterOutputStream) c.newInstance(outputStream);
      this.filterStream = new TimedCompressionOutputStream(lzoOutputStream, timeTakenNano);
    }
  }

  public static class CompressionFilterStreamProvider extends FilterStreamProvider.SimpleFilterReusableStreamProvider<TimedCompressionOutputStream, OutputStream>
  {
    private String compressorClassName;
    private MutableLong timeTakenNano;

    public CompressionFilterStreamProvider()
    {
      timeTakenNano = new MutableLong();
    }

    @Override
    public FilterStreamContext<TimedCompressionOutputStream> createFilterStreamContext(OutputStream outputStream)
    {
      try {
        return new CompressionFiltertreamContext(this.compressorClassName, outputStream, timeTakenNano);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void setCompressionClassName(String compressionClassName)
    {
      this.compressorClassName = compressionClassName;
    }

    public long getTimeTaken()
    {
      return timeTakenNano.longValue() / 1000;
    }
  }

}
