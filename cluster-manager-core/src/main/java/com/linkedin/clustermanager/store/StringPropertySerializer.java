package com.linkedin.clustermanager.store;

public class StringPropertySerializer implements PropertySerializer<String>
{
  @Override
  public byte[] serialize(String data) throws PropertyStoreException
  {
    if (data == null)
    {
      return null;
    }

    return data.getBytes();
  }

  @Override
  public String deserialize(byte[] bytes) throws PropertyStoreException
  {
    if (bytes == null)
    {
      return null;
    }
    
    return new String(bytes);
  }
}
