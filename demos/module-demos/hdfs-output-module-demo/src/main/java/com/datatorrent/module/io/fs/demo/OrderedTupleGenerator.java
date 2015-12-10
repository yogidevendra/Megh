package com.datatorrent.module.io.fs.demo;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;

public class OrderedTupleGenerator implements InputOperator
{
  private int messageSize = 1000;
  private int noOfMessages = 100*1000*1000;
  
  @Override
  public void beginWindow(long windowId)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setup(OperatorContext context)
  {
    
    
  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void emitTuples()
  {
    // TODO Auto-generated method stub
    
  }

}
