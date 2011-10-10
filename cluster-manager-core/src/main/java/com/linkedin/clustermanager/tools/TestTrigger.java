package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;


public class TestTrigger
{
  public long _startTime;
  public long _timeout;
  public ZnodeValue _expectValue;
  
  /**
   * no time or data trigger
   */
  public TestTrigger()
  {
    this(0, 0, (ZnodeValue)null);
  }

  /**
   * time trigger with a start time, no data trigger
   * @param startTime
   * @param timeout
   */
  public TestTrigger(long startTime)
  {
    this(startTime, 0, (ZnodeValue)null);
  }

  /**
   * simple field data trigger
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, String expect)
  {
    this(startTime, timeout, new ZnodeValue(expect));
  }

  /**
   * list field data trigger
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, List<String> expect)
  {
    this(startTime, timeout, new ZnodeValue(expect));
  }

  /**
   * map field data trigger
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, Map<String, String> expect)
  {
    this(startTime, timeout, new ZnodeValue(expect));
  }
  
  /**
   * znode data trigger
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, ZNRecord expect)
  {
    this(startTime, timeout, new ZnodeValue(expect));
  }
  
  /**
   * 
   * @param startTime
   * @param timeout
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, ZnodeValue expect)
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

  // TODO temp test; remove it
  public static void main(String[] args) 
  {
    TestTrigger trigger = new TestTrigger(0, 0, "simpleValue0");
    System.out.println("trigger=" + trigger);
    
    List<String> list = new ArrayList<String>();
    list.add("listValue1");
    list.add("listValue2");
    trigger = new TestTrigger(0, 0, list);
    System.out.println("trigger=" + trigger);
    
    Map<String, String> map = new HashMap<String, String>();
    map.put("mapKey3", "mapValue3");
    map.put("mapKey4", "mapValue4");
    trigger = new TestTrigger(0, 0, map);
    System.out.println("trigger=" + trigger);
    
    trigger = new TestTrigger();
    System.out.println("trigger=" + trigger);
  }
}
