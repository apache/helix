package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.clustermanager.tools.ZnodeModDesc.ZnodePropertyType;

public class ZnodeModCommand
{
  private ZnodeModTrigger _trigger;
  private String _znodePath;
  private ZnodePropertyType _propertyType;
  private String _operation;   // '+': update/create if not exist; '-': remove
  private String _key;
  // private String _updateValue;
  private ZnodeModValue _updateValue;
  
  public ZnodeModCommand()
  {
    _trigger = new ZnodeModTrigger();
  }

  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, String key, ZnodeModValue update)
  {
    this(new ZnodeModTrigger(), znodePath, type, op, key, update);
  }

  
  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, String key, ZnodeModValue expect, ZnodeModValue update)
  {
    this(new ZnodeModTrigger(expect), znodePath, type, op, key, update);
  }
  
  public ZnodeModCommand(long start, long timeout, String znodePath, ZnodePropertyType type, 
                         String op, String key, ZnodeModValue expect, ZnodeModValue update)
  {
    this(new ZnodeModTrigger(start, timeout, expect), znodePath, type, op, key, update);
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
    String ret = "command={ " + (_trigger == null? "<null>" : _trigger.toString()) + " | <\"" + 
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
    ZnodeModCommand command = new ZnodeModCommand();
    System.out.println(command);
    
    command = new ZnodeModCommand("/testPath", 
                                  ZnodePropertyType.SIMPLE, 
                                  "+", 
                                  "key1",
                                  new ZnodeModValue("value1"));
    System.out.println(command);
    
    List<String> list = new ArrayList<String>();
    list.add("value1");
    list.add("value2");
    command = new ZnodeModCommand("/testPath", 
                                  ZnodePropertyType.LIST, 
                                  "+", 
                                  "key1",
                                  new ZnodeModValue(list));
    System.out.println(command);
  }
}
