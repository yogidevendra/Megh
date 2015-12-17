package com.datatorrent.module.io.fs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.io.fs.AbstractFileInputOperator.FileLineInputOperator;

public class OutputValidator extends FileLineInputOperator
{
  private String previousFile;

  private Map<String, Long> fileNameToMaxIdMap = new HashMap<String, Long>();

  protected transient BufferedReader br;

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    br.close();
    br = null;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (pendingFileCount.longValue() == 0L) {
      throw new ShutdownException();
    }
  }

  @Override
  protected void emit(String tuple)
  {
    verify(tuple);
    output.emit(tuple);
  }

  private boolean verify(String tuple)
  {
    String[] tokens = tuple.split("\\|");
    long id = Long.parseLong(tokens[0]);
    String data = tokens[1];
    long crc = Long.parseLong(tokens[2]);

    if (!verifyCRC(id, data, crc) || !verifyOrder(id, data, crc)) {
      throw new ShutdownException();
    }
    return true;
  }

  public static void main(String[] args)
  {
    new OutputValidator().verify("1|ahfhsdjhfksh|890");
  }

  private static final CRC32 CRC = new CRC32();

  private boolean verifyCRC(long id, String data, long crc)
  {
    CRC.reset();
    CRC.update(data.getBytes());
    boolean compare = (crc == CRC.getValue());
    if (compare == false) {
      LOG.info("VALIDATION FAILED: CRC mismatch for id {} input {} output {} in file {}", id, crc, CRC.getValue(),
          currentFile);
    }
    return compare;
  }

  private boolean verifyOrder(long id, String data, long crc)
  {
    if (currentFile.equals(previousFile) == false) {
      if (fileNameToMaxIdMap.get(currentFile) == null) {
        fileNameToMaxIdMap.put(currentFile, 0L);
      }
      previousFile = currentFile;
    }

    long visitedId = fileNameToMaxIdMap.get(currentFile);
    fileNameToMaxIdMap.put(currentFile, id);

    if (visitedId > id) {
      LOG.info("VALIDATION FAILED: visitedId {} exists before id {} in file {}", visitedId, id, currentFile);
      return false;
    }
    return true;
  }

  private static final Logger LOG = LoggerFactory.getLogger(OutputValidator.class);

}
