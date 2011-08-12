package com.linkedin.clustermanager.tools;

import com.linkedin.clustermanager.tools.ZnodeModDesc.ZnodePropertyType;

public class ZnodeModVerifier
{
  public long _timeout;
  public String _znodePath;
  public ZnodePropertyType _propertyType;
  public String _operation;   // "==", "!="
  public String _key;
  public String _value;
  
  public ZnodeModVerifier()
  {
    
  }
  
  public ZnodeModVerifier(String path, ZnodePropertyType type, 
                          String op, String key, String value)
  {
    this(0, path, type, op, key, value);
  }
 
  public ZnodeModVerifier(long timeout, String path, ZnodePropertyType type, 
                          String op, String key, String value)
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
  
  //temp test
  public static void main(String[] args) 
  {
    ZnodeModVerifier verifier = new ZnodeModVerifier("/TEST_CASES", 
                                                     ZnodePropertyType.SIMPLE, 
                                                     "==", 
                                                     "key1", 
                                                     "value1");  
    System.out.println(verifier);
    
    verifier = new ZnodeModVerifier();
    System.out.println(verifier);
  }
}
