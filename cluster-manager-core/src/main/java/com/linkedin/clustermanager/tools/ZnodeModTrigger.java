package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ZnodeModTrigger
{
  private long _startTime;
  private long _timeout;
  // private String _expectValue;
  private ZnodeModValue _expectValue;
  
  
  public ZnodeModTrigger()
  {
    this(0, 0, null);
  }

  public ZnodeModTrigger(long startTime, long timeout)
  {
    
    this(startTime, timeout, null);
  }
  
  public ZnodeModTrigger(ZnodeModValue expect)
  {
    this(0, 0, expect);
  }
  
  public ZnodeModTrigger(long startTime, long timeout, ZnodeModValue expect)
  {
    _startTime = startTime;
    _timeout = timeout;
    _expectValue = expect;
  }
 
  @Override
  public String toString()
  {
    String ret = "<" + _startTime + "~" + _timeout + "ms, " + _expectValue + ">";
    return ret;
  }

  // setter/getter's
  public void setStartTime(long startTime)
  {
    _startTime = startTime;
  }
  
  public long getStartTime()
  {
    return _startTime;
  }
  
  public void setTimeout(long timeout)
  {
    _timeout = timeout;
  }
  
  public long getTimeout()
  {
    return _timeout;
  }
  
  public void setExpectValue(ZnodeModValue expect)
  {
    _expectValue = expect;
  }
  
  public ZnodeModValue getExpectValue()
  {
    return _expectValue;
  }
  
  // temp test
  public static void main(String[] args) 
  {
    ZnodeModTrigger trigger = new ZnodeModTrigger(new ZnodeModValue("value0"));
    System.out.println("trigger=" + trigger);
    
    List<String> list = new ArrayList<String>();
    list.add("value1");
    list.add("value2");
    trigger = new ZnodeModTrigger(new ZnodeModValue(list));
    System.out.println("trigger=" + trigger);
    
    Map<String, String> map = new HashMap<String, String>();
    map.put("key3", "value3");
    map.put("key4", "value4");
    trigger = new ZnodeModTrigger(new ZnodeModValue(map));
    System.out.println("trigger=" + trigger);
    
    trigger = new ZnodeModTrigger(new ZnodeModValue());
    System.out.println("trigger=" + trigger);
  }
}
