package com.linkedin.clustermanager.store;

public interface PropertySerializer<T>
{
  public byte[] serialize(T data) throws PropertyStoreException;

  public T deserialize(byte[] bytes) throws PropertyStoreException;
}
