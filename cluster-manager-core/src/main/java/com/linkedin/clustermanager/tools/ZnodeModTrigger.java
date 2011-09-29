package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;


public class ZnodeModTrigger
{
  private long _startTime;
  private long _timeout;
  private ZnodeModValue _expectValue;
  
  /**
   * no time or data trigger
   */
  public ZnodeModTrigger()
  {
    this(0, 0, (ZnodeModValue)null);
  }

  /**
   * time trigger with a start time and a timeout, no data trigger
   * @param startTime
   * @param timeout
   */
  public ZnodeModTrigger(long startTime, long timeout)
  {
    this(startTime, timeout, (ZnodeModValue)null);
  }

  /**
   * simple field data trigger, no time trigger
   * @param expect
   */
  public ZnodeModTrigger(String expect)
  {
    this(0, 0, new ZnodeModValue(expect));
  }

  /**
   * list field data trigger, no time trigger
   * @param expect
   */
  public ZnodeModTrigger(List<String> expect)
  {
    this(0, 0, new ZnodeModValue(expect));
  }

  /**
   * map field data trigger, no time trigger
   * @param expect
   */
  public ZnodeModTrigger(Map<String, String> expect)
  {
    this(0, 0, new ZnodeModValue(expect));
  }
  
  /**
   * znode data trigger, no time trigger
   * @param expect
   */
  public ZnodeModTrigger(ZNRecord expect)
  {
    this(0, 0, new ZnodeModValue(expect));
  }
  
  /**
   * simple field with time and data trigger
   * @param start
   * @param timeout
   * @param expect
   */
  public ZnodeModTrigger(long start, long timeout, String expect)
  {
    this(start, timeout, new ZnodeModValue(expect));
  }

  /**
   * znode with time and data trigger
   * @param start
   * @param timeout
   * @param expect
   */
  public ZnodeModTrigger(long start, long timeout, ZNRecord expect)
  {
    this(start, timeout, new ZnodeModValue(expect));
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
    ZnodeModTrigger trigger = new ZnodeModTrigger("simpleValue0");
    System.out.println("trigger=" + trigger);
    
    List<String> list = new ArrayList<String>();
    list.add("listValue1");
    list.add("listValue2");
    trigger = new ZnodeModTrigger(list);
    System.out.println("trigger=" + trigger);
    
    Map<String, String> map = new HashMap<String, String>();
    map.put("mapKey3", "mapValue3");
    map.put("mapKey4", "mapValue4");
    trigger = new ZnodeModTrigger(map);
    System.out.println("trigger=" + trigger);
    
    trigger = new ZnodeModTrigger();
    System.out.println("trigger=" + trigger);
  }
}
