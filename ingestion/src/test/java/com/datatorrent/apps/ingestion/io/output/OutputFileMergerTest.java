/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io.output;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputBlock;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputFileBlockMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.fs.FileSplitter.FileMetadata;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.google.common.collect.Lists;


/**
 * Unit tests for OutputFileMerger
 */
public class OutputFileMergerTest
{
  public static final String[] FILE_CONTENTS = {
    "abcdefghi", "pqr", "hello world", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "0123456789"};
  
  public static final int BLOCK_SIZE = 5;
  

  private class TestMeta extends TestWatcher
  {
    String outputPath;
    List<FileMetadata> fileMetadataList = Lists.newArrayList();
    
    OutputFileMerger<IngestionFileMetaData> oper;
    File blocksDir;
    Context.OperatorContext context;
    
    /* (non-Javadoc)
     * @see org.junit.rules.TestWatcher#starting(org.junit.runner.Description)
     */
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      outputPath = new File("target/" + description.getClassName() + "/" + description.getMethodName()).getPath();
      
      oper = new OutputFileMerger<IngestionFileMetaData>();
      oper.setFilePath(outputPath);
      String appDirectory = outputPath;
      
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, description.getClassName());
      attributes.put(DAG.DAGContext.APPLICATION_PATH, appDirectory);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      
      oper.setup(context);
      
      try {
        File outDir = new File(outputPath);
        FileUtils.forceMkdir(outDir);
        
        blocksDir = new File(context.getValue(Context.DAGContext.APPLICATION_PATH) , BlockWriter.SUBDIR_BLOCKS);
        blocksDir.mkdirs();
        
        long blockID=1000;
        
        for (int i = 0; i < FILE_CONTENTS.length; i++) {
          List<Long> blockIDs = Lists.newArrayList();
          
          File file = new File(outputPath, i + ".txt");

          FileUtils.write(file, FILE_CONTENTS[i]);

          
          int offset=0;
          for(; offset< FILE_CONTENTS[i].length(); offset+= BLOCK_SIZE, blockID++){
            String blockContents;
            if(offset+BLOCK_SIZE < FILE_CONTENTS[i].length()){
              blockContents= FILE_CONTENTS[i].substring(offset, offset+BLOCK_SIZE);
            }
            else{
              blockContents= FILE_CONTENTS[i].substring(offset);
            }
            FileUtils.write(new File(blocksDir, blockID+""), blockContents);
            blockIDs.add(blockID);
          }
          
          FileMetadata fileMetadata = new FileMetadata(file.getPath());
          fileMetadata.setBlockIds(ArrayUtils.toPrimitive(blockIDs.toArray(new Long[0])));
          fileMetadataList.add(fileMetadata);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    /* (non-Javadoc)
     * @see org.junit.rules.TestWatcher#finished(org.junit.runner.Description)
     */
    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      
      try {
        FileUtils.deleteDirectory(new File(outputPath));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Rule
  public TestMeta testMeta = new TestMeta();
  
  
  @Test
  public void testFileMerger() throws IOException, InterruptedException{
    long[][][] partitionMeta= {
        {{1000,0,5},{1001,0,4}},
        {{1002,0,3}},//testing multiple blocks from same block file
        {{1003,0,5},{1004,0,5},{1005,0,1}},
        {{1006,0,5},{1007,0,5},{1008,0,5},{1009,0,5},{1010,0,5},{1011,0,1}},
        {{1012,0,5},{1013,0,5}}
    };
    
    testMeta.oper.beginWindow(0);
    long fileID = 0;
    for(int tupleIndex=0; tupleIndex<partitionMeta.length; tupleIndex++){
      IngestionFileMetaData ingestionFileMetaData = new IngestionFileMetaData();
      String fileName = fileID++ +".txt";
      ingestionFileMetaData.setRelativePath(fileName);
      List<OutputBlock> outputBlocks = Lists.newArrayList();
      for(long[] block: partitionMeta[tupleIndex]){
        String blockFilePath = new Path(testMeta.outputPath, Long.toString(block[0])).toString();
        FileBlockMetadata fileBlockMetadata = new FileBlockMetadata(blockFilePath, block[0], block[1], block[2], 
            false, -1);
        OutputFileBlockMetaData outputFileBlockMetaData = new OutputFileBlockMetaData(fileBlockMetadata, fileName, false);
        outputBlocks.add(outputFileBlockMetaData);
      }
      ingestionFileMetaData.setOutputBlockMetaDataList(outputBlocks);
      testMeta.oper.input.process(ingestionFileMetaData);
    }
    testMeta.oper.endWindow();
    testMeta.oper.committed(0);
    //give some time to complete postCommit operations
    Thread.sleep(2*1000);
    
    for(int fileId=0; fileId < partitionMeta.length; fileId++ ){
      String fromFile = FileUtils.readFileToString(new File(testMeta.oper.getFilePath(),fileId+".txt"));
      Assert.assertEquals("File "+ fileId+"not matching", FILE_CONTENTS[fileId], fromFile);
    }
  }
  
  
  
}