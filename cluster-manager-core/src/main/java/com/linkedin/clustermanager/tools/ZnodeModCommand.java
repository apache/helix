package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.tools.ZnodeModDesc.ZnodePropertyType;

public class ZnodeModCommand
{
  private ZnodeModTrigger _trigger;
  private String _znodePath;
  private ZnodePropertyType _propertyType;
  private String _operation;   // '+': update/create if not exist; '-': remove
  private String _key;
  private ZnodeModValue _updateValue;
  
  public ZnodeModCommand()
  {
    _trigger = new ZnodeModTrigger();
  }
  
  /**
   * simple field change without time or data trigger
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param update
   */
  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, String key, String update)
  {
    this(new ZnodeModTrigger(), znodePath, type, op, key, new ZnodeModValue(update));
  }

  /**
   * list field change without time or data trigger
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param update
   */
  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, String key, List<String> update)
  {
    this(new ZnodeModTrigger(), znodePath, type, op, key, new ZnodeModValue(update));
  }

  /**
   * map field change without time or data trigger
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param update
   */
  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, String key, Map<String, String> update)
  {
    this(new ZnodeModTrigger(), znodePath, type, op, key, new ZnodeModValue(update));
  }

  /**
   * znode change without time or data trigger 
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param update
   */
  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, ZNRecord update)
  {
    this(new ZnodeModTrigger(), znodePath, type, op, null, new ZnodeModValue(update));
  }

  /**
   * simple field change with data trigger
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param expect
   * @param update
   */
  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, String key, String expect, String update)
  {
    this(new ZnodeModTrigger(expect), znodePath, type, op, key, new ZnodeModValue(update));
  }

  /**
   * list change with data trigger
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param expect
   * @param update
   */
  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, String key, List<String> expect, List<String> update)
  {
    this(new ZnodeModTrigger(expect), znodePath, type, op, key, new ZnodeModValue(update));
  }

  /**
   * znode change with data trigger
   * @param znodePath
   * @param type
   * @param op
   * @param expect
   * @param update
   */
  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, ZNRecord expect, ZNRecord update)
  {
    this(new ZnodeModTrigger(expect), znodePath, type, op, null, new ZnodeModValue(update));
  }

  /**
   * simple field change with time and data trigger
   * @param start
   * @param timeout
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param expect
   * @param update
   */
  public ZnodeModCommand(long start, long timeout, String znodePath, ZnodePropertyType type, 
                         String op, String key, String expect, String update)
  {
    this(new ZnodeModTrigger(start, timeout, expect), znodePath, type, op, key, new ZnodeModValue(update));
  }

  /**
   * znode change with time and data trigger
   * @param start
   * @param timeout
   * @param znodePath
   * @param type
   * @param op
   * @param expect
   * @param update
   */
  public ZnodeModCommand(long start, long timeout, String znodePath, ZnodePropertyType type, 
                         String op, ZNRecord expect, ZNRecord update)
  {
    this(new ZnodeModTrigger(start, timeout, expect), znodePath, type, op, null, new ZnodeModValue(update));
  }
  
  /**
   * znode with time trigger, no data trigger
   * @param start
   * @param timeout
   * @param znodePath
   * @param type
   * @param op
   * @param update
   */
  public ZnodeModCommand(long start, long timeout, String znodePath, ZnodePropertyType type, 
                         String op, ZNRecord update)
  {
    this(new ZnodeModTrigger(start, timeout), znodePath, type, op, null, new ZnodeModValue(update));
  }
  
  public ZnodeModCommand(ZnodeModTrigger trigger, String znodePath, ZnodePropertyType type, 
                         String op, String key, ZnodeModValue update)
  {
    _trigger = trigger;
    _znodePath = znodePath;
    _propertyType = type;
    _operation = op;
    _key = key;
    _updateValue = update;
    
  }
  
  public String toString()
  {
    String ret = super.toString() + "={ " + (_trigger == null? "<null>" : _trigger.toString()) + " | <\"" + 
                 _znodePath + "\", " + _propertyType + "/" + _key + " " + _operation + " " +
                 _updateValue + "> }";
    return ret;
  }
  
  // getter/setter's
  public void setTrigger(ZnodeModTrigger trigger)
  {
    _trigger = trigger;
  }
  
  public ZnodeModTrigger getTrigger()
  {
    return _trigger;
  }
  
  public void setZnodePath(String path)
  {
    _znodePath = path;
  }
  
  public String getZnodePath()
  {
    return _znodePath;
  }
  
  public void setPropertyType(ZnodePropertyType type)
  {
    _propertyType = type;
  }
  
  public ZnodePropertyType getPropertyType()
  {
    return _propertyType;
  }
  
  public void setOperation(String op)
  {
    _operation = op;
  }
  
  public String getOperation()
  {
    return _operation;
  }
  
  public void setKey(String key)
  {
    _key = key;
  }
  
  public String getKey()
  {
    return _key;
  }
  
  public void setUpdateValue(ZnodeModValue update)
  {
    _updateValue = update;
  }
  
  public ZnodeModValue getUpdateValue()
  {
    return _updateValue;
  }
  
  // temp test
  public static void main(String[] args) 
  {
    // null modification command
    ZnodeModCommand command = new ZnodeModCommand();
    System.out.println(command);
    
    // simple modification command
    command = new ZnodeModCommand("/testPath", ZnodePropertyType.SIMPLE, 
                                  "+", "key1", "simpleValue1");
    System.out.println(command);
    
    // list modification command
    List<String> list = new ArrayList<String>();
    list.add("listValue1");
    list.add("listValue2");
    command = new ZnodeModCommand("/testPath", ZnodePropertyType.LIST, 
                                  "+", "key1", list);
    System.out.println(command);
    
    // map modification command
    Map<String, String> map = new HashMap<String, String>();
    map.put("mapKey1", "mapValue1");
    map.put("mapKey2", "mapValue2");
    command = new ZnodeModCommand("/testPath", ZnodePropertyType.MAP, 
                                  "+", "key1", map);
    System.out.println(command);

    // map modification command
    ZNRecord record = new ZNRecord();
    record.setId("znrecord");
    record.setSimpleField("key1", "simpleValue1");
    record.setListField("key1", list);
    record.setMapField("key1", map);
    command = new ZnodeModCommand("/testPath", ZnodePropertyType.ZNODE, 
                                  "+", record);
    System.out.println(command);
  }
}
