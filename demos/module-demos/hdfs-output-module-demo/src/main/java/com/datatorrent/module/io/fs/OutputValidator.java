package com.datatorrent.module.io.fs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.FileLineInputOperator;
import com.datatorrent.module.io.fs.OrderedTupleGenerator.MessageWithCRCCheck;

public class OutputValidator extends FileLineInputOperator
{
  private String previousFile;

  private Map<String, Long> fileNameToMaxIdMap = new HashMap<String, Long>();

  protected transient BufferedReader br;

  public final transient DefaultOutputPort<MessageWithCRCCheck> messages = new DefaultOutputPort<MessageWithCRCCheck>();

  private static final String STATUS_FILE = "INTEGRITY.success";
  Path statusPath;

  OutputValidator()
  {

  }

  @Override
  public void setup(OperatorContext context)
  {
    LOG.debug("APPLICATION_PATH:{}", context.getValue(DAG.APPLICATION_PATH));
    LOG.debug("STATUS_FILE", STATUS_FILE);
    statusPath = new Path(context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + STATUS_FILE);
    LOG.debug("statusPath:{}", statusPath);
    super.setup(context);

  }

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

      try {
        fs.create(statusPath, true);
      } catch (IOException e) {
        throw new RuntimeException();
      }

      LOG.info("VALIDATION SUCCESS: PASSED tuple integrity check");
      throw new ShutdownException();
    }
  }

  @Override
  protected void emit(String tuple)
  {
    MessageWithCRCCheck messageWithCRCCheck = new MessageWithCRCCheck(tuple);
    verify(messageWithCRCCheck);
    messages.emit(messageWithCRCCheck);
  }

  private boolean verify(MessageWithCRCCheck messageWithCRCCheck)
  {
    if (!verifyCRC(messageWithCRCCheck) || !verifyOrder(messageWithCRCCheck)) {
      //throw new ShutdownException();
    }
    return true;
  }

  private boolean verifyCRC(MessageWithCRCCheck messageWithCRCCheck)
  {
    long computed = MessageWithCRCCheck.computeCRC(messageWithCRCCheck.getData());
    boolean compare = (computed == messageWithCRCCheck.getCrc());
    if (compare == false) {
      LOG.info("VALIDATION FAILED: CRC mismatch for id {} input {} output {} in file {}", messageWithCRCCheck.getId(),
          messageWithCRCCheck.getCrc(), computed, currentFile);
    }
    return compare;
  }

  private boolean verifyOrder(MessageWithCRCCheck messageWithCRCCheck)
  {
    if (currentFile.equals(previousFile) == false) {
      if (fileNameToMaxIdMap.get(currentFile) == null) {
        fileNameToMaxIdMap.put(currentFile, 0L);
      }
      previousFile = currentFile;
    }

    long visitedId = fileNameToMaxIdMap.get(currentFile);
    fileNameToMaxIdMap.put(currentFile, messageWithCRCCheck.getId());

    if (visitedId > messageWithCRCCheck.getId()) {
      LOG.info("VALIDATION FAILED: visitedId {} exists before id {} in file {}", visitedId, messageWithCRCCheck.getId(),
          currentFile);
      return false;
    }
    return true;
  }

  private static final Logger LOG = LoggerFactory.getLogger(OutputValidator.class);

}
