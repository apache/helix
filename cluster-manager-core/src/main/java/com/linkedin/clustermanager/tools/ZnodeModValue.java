package com.linkedin.clustermanager.tools;

import java.util.List;
import java.util.Map;

public class ZnodeModValue
{
  private String _singleValue;
  private List<String> _listValue;
  private Map<String, String> _mapValue;
  
  public ZnodeModValue()
  {
    
  }
  
  public ZnodeModValue(String value)
  {
    _singleValue = value;
  }
  
  public ZnodeModValue(List<String> value)
  {
    _listValue = value;
  }
  
  public ZnodeModValue(Map<String, String> value)
  {
    _mapValue = value;
  }
  
  // getter/setter's
  public void setSingleValue(String value)
  {
    _singleValue = value;
  }
  
  public String getSingleValue()
  {
    return _singleValue;
  }
  
  public void setListValue(List<String> value)
  {
    _listValue = value;
  }
  
  public List<String> getListValue()
  {
    return _listValue;
  }
  
  public void setMapValue(Map<String, String> value)
  {
    _mapValue = value;
  }
  
  public Map<String, String> getMapValue()
  {
    return _mapValue;
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
    
    return "null";
  }
}
