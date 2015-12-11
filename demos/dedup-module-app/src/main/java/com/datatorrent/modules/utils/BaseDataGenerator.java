package com.datatorrent.modules.utils;

import java.text.MessageFormat;
import java.util.Random;

public class BaseDataGenerator
{
  int n = 1000000;

  public Usecase usecase;
  // Format: ID, UID, EXP, DECISION
  public String template = "{0,number,#}|{1,number,#}|{2}|{3}|{4}";
  public MessageFormat mf;
  protected Random r;
  protected int id = 0;
  protected int index = 0;

  public BaseDataGenerator(int n) {
    this.n = n;
    r = new Random();
    mf = new MessageFormat(template);
  }

  public String generateNextTuple() {
    String retVal = "";
    if(r.nextInt() % 10 == 0 && index > 0) {
      retVal = MessageFormat.format(template, r.nextInt(id), index, "", "", "DUPLICATE");
    }
    else {
      retVal = MessageFormat.format(template, id, index, "", "", "UNIQUE");
      id++;
    }
    index++;
    return retVal;
  }

  public enum Usecase {
    ORDERED,
    TUPLETIME,
    SYSTEMTIME,
    CATEGORICAL
  }

}
