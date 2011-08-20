package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.clustermanager.tools.ZnodeModDesc.ZnodePropertyType;

public class ZnodeModVerifier
{
  private long _timeout;
  private String _znodePath;
  private ZnodePropertyType _propertyType;
  private String _operation;   // "==", "!="
  private String _key;
  // private String _value;
  private ZnodeModValue _value; 
  
  public ZnodeModVerifier()
  {
    
  }
  
  public ZnodeModVerifier(String path, ZnodePropertyType type, 
                          String op, String key, ZnodeModValue value)
  {
    this(0, path, type, op, key, value);
  }
 
  public ZnodeModVerifier(long timeout, String path, ZnodePropertyType type, 
                          String op, String key, ZnodeModValue value)
  {
    _timeout = timeout;
    _znodePath = path;
    _propertyType = type;
    _operation = op;
    _key = key;
    _value = value;
  }
  
  public String toString()
  {
    String ret = "verifier={ " + _timeout + "ms, \"" +  
                 _znodePath + "\", " + _propertyType + "/" + _key + " " + _operation + 
                 " " + _value + " }";
    return ret;
  }
  
  // setter/getter's
  public void setTimeout(long timeout)
  {
    _timeout = timeout;
  }
  
  public long getTimeout()
  {
    return _timeout;
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
  
  public void setValue(ZnodeModValue value)
  {
    _value = value;
  }
  
  public ZnodeModValue getValue()
  {
    return _value;
  }
  
  //temp test
  public static void main(String[] args) 
  {
    ZnodeModVerifier verifier = new ZnodeModVerifier("/testPath", 
                                                     ZnodePropertyType.SIMPLE, 
                                                     "==", 
                                                     "key1", 
                                                     new ZnodeModValue("value1"));  
    System.out.println(verifier);
    
    verifier = new ZnodeModVerifier("/testPath", 
                                    ZnodePropertyType.LIST, 
                                    "==", 
                                    "key1/0",
                                    new ZnodeModValue("value1"));  
    System.out.println(verifier);

    List<String> list = new ArrayList<String>();
    list.add("value1");
    list.add("value2");
    verifier = new ZnodeModVerifier("/testPath", 
                                    ZnodePropertyType.LIST, 
                                    "==", 
                                    "key1",
                                    new ZnodeModValue(list));  
    System.out.println(verifier);
    
    verifier = new ZnodeModVerifier();
    System.out.println(verifier);
  }
}
