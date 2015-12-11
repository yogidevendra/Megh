package com.datatorrent.modules.utils;

import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

public class TimeDataGenerator extends OrderedDataGenerator
{
  // Format: ID, UID, EXP
  public String template = "{0,number,#}|{1,number,#}|{2}|{3,date,yyyy-MM-dd HH:mm:ss}|{4}";
  protected long expiry = new Date().getTime(); // millis
  protected int expiryPeriod = 60*1000; // 1 min
  protected long expiryIds[];

  public TimeDataGenerator(int n, int expiryPeriod)
  {
    super(n, expiryPeriod);
    this.expiryPeriod = expiryPeriod;
    expiryIds = new long[n];
  }

  @Override
  public String generateNextTuple()
  {
    String retVal = "";
    if(r.nextInt() % 10 == 0 && index > 0) { // Duplicates and Expired
      int dupId = getDuplicateId();
      if(dupId <= id && dupId >= 0) {
        retVal = MessageFormat.format(template, dupId, index, "", expiryIds[dupId], expiry - expiryIds[dupId] > expiryPeriod ? "EXPIRED" : "DUPLICATE");
      }
    }
    else {
      updateExpiry();
      retVal = MessageFormat.format(template, id, index, "",expiry,"UNIQUE");
      id++;
      expiryIds[id] = expiry;
    }
    index++;
    return retVal;
  }

  @Override
  public int getDuplicateId()
  {
    int dupId = ThreadLocalRandom.current().nextInt(id - expiryPeriod/1000, id + 1);
    return dupId;
  }
  public void updateExpiry()
  {
    expiry += r.nextInt(4)*1000;
  }
}
