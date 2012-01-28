package com.linkedin.helix.store;

public class PropertyStat
{
  private long _lastModifiedTime;   // time in milliseconds from epoch when this property was last modified
  private int _version;   // latest version number
  
  public PropertyStat()
  {
    this(0, 0);
  }
  
  public PropertyStat(long lastModifiedTime, int version)
  {
    _lastModifiedTime = lastModifiedTime;
    _version = version;
  }
    
  public long getLastModifiedTime()
  {
    return _lastModifiedTime;
  }
  
  public int getVersion()
  {
    return _version;
  }
  
  public void setLastModifiedTime(long lastModifiedTime)
  {
    
    _lastModifiedTime = lastModifiedTime;
  }
  
  public void setVersion(int version)
  {
    _version = version;
  }
}
