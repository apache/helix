package com.linkedin.clustermanager.tools;


public class ZnodeModTrigger
{
  public long _startTime;
  public long _timeout;
  public String _expectValue;
  
  public ZnodeModTrigger()
  {
    this(0, 0, null);
  }

  public ZnodeModTrigger(long startTime, long timeout)
  {
    
    this(startTime, timeout, null);
  }
  
  public ZnodeModTrigger(String expect)
  {
    this(0, 0, expect);
  }
  
  public ZnodeModTrigger(long startTime, long timeout, String expect)
  {
    _startTime = startTime;
    _timeout = timeout;
    _expectValue = expect;
  }
 
  @Override
  public String toString()
  {
    String ret = "<" + _startTime + "-" + _timeout + "ms, " + _expectValue + ">";
    return ret;
  }
  
  // temp test
  public static void main(String[] args) 
  {
    ZnodeModTrigger trigger = new ZnodeModTrigger("value0");
    System.out.println("trigger=" + trigger);
  }
}
