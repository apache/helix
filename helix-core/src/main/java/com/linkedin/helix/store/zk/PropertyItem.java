package com.linkedin.helix.store.zk;

import org.apache.zookeeper.data.Stat;

public class PropertyItem
{
  static class ByteArray
  {
    byte[] _bytes;
    
    public ByteArray(byte[] bytes)
    {
      if (bytes == null)
      {
        throw new IllegalArgumentException("bytes can't be null");
      }
      _bytes = bytes;
    }
    
    public byte[] getBytes()
    {
      return _bytes;
    }
  }
  
  ByteArray _value;
  Stat _stat;
  
  public PropertyItem(byte[] value, Stat stat)
  {
    _value = new ByteArray(value);
    _stat = stat;
  }
  
  public byte[] getBytes()
  {
    return _value._bytes;
  }
  
  public int getVersion()
  {
    return _stat.getVersion();
  }
  
  public long getLastModifiedTime()
  {
    return _stat.getMtime();
  }
}
