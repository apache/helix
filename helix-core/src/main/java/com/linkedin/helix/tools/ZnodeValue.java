package com.linkedin.helix.tools;

import java.util.List;
import java.util.Map;

import com.linkedin.helix.ZNRecord;

public class ZnodeValue
{
  public String _singleValue;
  public List<String> _listValue;
  public Map<String, String> _mapValue;
  public ZNRecord _znodeValue;
  
  public ZnodeValue()
  {
  }
  
  public ZnodeValue(String value)
  {
    _singleValue = value;
  }
  
  public ZnodeValue(List<String> value)
  {
    _listValue = value;
  }
  
  public ZnodeValue(Map<String, String> value)
  {
    _mapValue = value;
  }
  
  public ZnodeValue(ZNRecord value)
  {
    _znodeValue = value;
  }
  
  @Override
  public String toString()
  {
    if (_singleValue != null)
    {
      return _singleValue;
    }
    else if (_listValue != null)
    {
      return _listValue.toString();
    }
    else if (_mapValue != null)
    {
      return _mapValue.toString();
    }
    else if (_znodeValue != null)
    {
      return _znodeValue.toString();
    }
    
    return "null";
  }
}
