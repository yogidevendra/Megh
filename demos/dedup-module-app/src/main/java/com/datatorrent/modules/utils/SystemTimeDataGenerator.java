package com.datatorrent.modules.utils;

public class SystemTimeDataGenerator extends TimeDataGenerator
{
  public SystemTimeDataGenerator(int n, int expiryPeriod)
  {
    super(n, expiryPeriod);
  }

  @Override
  public int getDuplicateId()
  {
    int dupId = r.nextInt(id);
    return dupId;
  }

  public void updateExpiry()
  {
    expiry = System.currentTimeMillis();
  }
}
