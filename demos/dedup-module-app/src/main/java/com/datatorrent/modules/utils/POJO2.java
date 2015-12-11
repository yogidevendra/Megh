package com.datatorrent.modules.utils;

import java.util.Date;

public class POJO2
{
  public long id;
  public long uid;
  public long expId;
  public Date date;
  public String decision;

  public long getId()
  {
    return id;
  }
  public void setId(long id)
  {
    this.id = id;
  }
  public long getUid()
  {
    return uid;
  }
  public void setUid(long uid)
  {
    this.uid = uid;
  }
  public long getExpId()
  {
    return expId;
  }
  public void setExpId(long expId)
  {
    this.expId = expId;
  }
  public String getDecision()
  {
    return decision;
  }
  public void setDecision(String decision)
  {
    this.decision = decision;
  }
}
