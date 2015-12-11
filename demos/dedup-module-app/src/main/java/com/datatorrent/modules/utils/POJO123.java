package com.datatorrent.modules.utils;

import java.util.Date;

public class POJO123
{
  public String name;
  public String phone;
  public String email;
  public Date date;
  public String city;
  public long pin;
  public String country;
  public long num1;
  public double num2;
  public String guid;
  public String tag;
  public long id;
  public long dupId;
  public long expId;

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public String getPhone()
  {
    return phone;
  }

  public void setPhone(String phone)
  {
    this.phone = phone;
  }

  public String getEmail()
  {
    return email;
  }

  public void setEmail(String email)
  {
    this.email = email;
  }

  public Date getDate()
  {
    return date;
  }

  public void setDate(Date date)
  {
    this.date = date;
  }

  public String getCity()
  {
    return city;
  }

  public void setCity(String city)
  {
    this.city = city;
  }

  public long getPin()
  {
    return pin;
  }

  public void setPin(long pin)
  {
    this.pin = pin;
  }

  public String getCountry()
  {
    return country;
  }

  public void setCountry(String country)
  {
    this.country = country;
  }

  public long getNum1()
  {
    return num1;
  }

  public void setNum1(long num1)
  {
    this.num1 = num1;
  }

  public double getNum2()
  {
    return num2;
  }

  public void setNum2(double num2)
  {
    this.num2 = num2;
  }

  public String getGuid()
  {
    return guid;
  }

  public void setGuid(String guid)
  {
    this.guid = guid;
  }

  public String getTag()
  {
    return tag;
  }

  public void setTag(String tag)
  {
    this.tag = tag;
  }

  public long getId()
  {
    return id;
  }

  public void setId(long id)
  {
    this.id = id;
  }

  public long getDupId()
  {
    return dupId;
  }

  public void setDupId(long dupId)
  {
    this.dupId = dupId;
  }

  public long getExpId()
  {
    return expId;
  }

  public void setExpId(long expId)
  {
    this.expId = expId;
  }

  @Override
  public String toString()
  {
    return id + ", " + expId + ", " + name + ", " + ", " + date + ", " + tag + "\n";
  }
}
