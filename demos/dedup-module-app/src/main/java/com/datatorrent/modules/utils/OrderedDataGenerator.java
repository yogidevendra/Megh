package com.datatorrent.modules.utils;

import java.text.MessageFormat;
import java.util.concurrent.ThreadLocalRandom;

public class OrderedDataGenerator extends BaseDataGenerator
{
  // Format: ID, UID, EXP
  public String template = "{0,number,#}|{1,number,#}|{2,number,#}|{3}|{4}";
  protected int expiry = 0;
  protected int expiryPeriod = 1000;
  protected long expiryIds[];
  protected int expiryIncrement = 4; // Incrementing 0 to 3 randomly.

  public OrderedDataGenerator(int n, int expiryPeriod)
  {
    super(n);
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
        retVal = MessageFormat.format(template, dupId, index, expiryIds[dupId], "", expiry - expiryIds[dupId] > expiryPeriod ? "EXPIRED" : "DUPLICATE");
      }
    }
    else {
      updateExpiry();
      retVal = MessageFormat.format(template, id, index, expiry, "", "UNIQUE");
      id++;
      expiryIds[id] = expiry;
    }
    index++;
    return retVal;
  }

  public int getDuplicateId()
  {
    int dupId = ThreadLocalRandom.current().nextInt(id - expiryPeriod, id + 1);
    return dupId;
  }

  public void updateExpiry()
  {
    expiry += r.nextInt(expiryIncrement);
  }
}
