package com.linkedin.clustermanager.store;

public interface PropertySerializer
{
  public byte[] serialize(Object data) throws PropertyStoreException;

  public Object deserialize(byte[] bytes) throws PropertyStoreException;
}
