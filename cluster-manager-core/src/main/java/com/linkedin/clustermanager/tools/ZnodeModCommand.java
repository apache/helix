package com.linkedin.clustermanager.tools;

import com.linkedin.clustermanager.tools.ZnodeModDesc.ZnodePropertyType;

public class ZnodeModCommand
{
  public ZnodeModTrigger _trigger;
  public String _znodePath;
  public ZnodePropertyType _propertyType;
  public String _operation;   // '+': update/create if not exist; '-': remove
  public String _key;
  public String _updateValue;
  
  public ZnodeModCommand()
  {
    _trigger = new ZnodeModTrigger();
  }

  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, String key, String update)
  {
    this(new ZnodeModTrigger(), znodePath, type, op, key, update);
  }

  
  public ZnodeModCommand(String znodePath, ZnodePropertyType type, 
                         String op, String key, String expect, String update)
  {
    this(new ZnodeModTrigger(expect), znodePath, type, op, key, update);
  }
  
  public ZnodeModCommand(long start, long timeout, String znodePath, ZnodePropertyType type, 
                         String op, String key, String expect, String update)
  {
    this(new ZnodeModTrigger(start, timeout, expect), znodePath, type, op, key, update);
  }
  
  public ZnodeModCommand(ZnodeModTrigger trigger, String znodePath, ZnodePropertyType type, 
                         String op, String key, String update)
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
  
  // temp test
  public static void main(String[] args) 
  {
    ZnodeModCommand command = new ZnodeModCommand();
    System.out.println(command);
    
    command = new ZnodeModCommand("TEST_CASES", 
                                  ZnodePropertyType.SIMPLE, 
                                  "+", 
                                  "key1",
                                  "value1");
    System.out.println(command);
  }
}
