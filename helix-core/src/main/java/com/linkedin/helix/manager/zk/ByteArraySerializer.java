package com.linkedin.helix.manager.zk;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class ByteArraySerializer implements ZkSerializer
{
  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError
  {
    return (byte[])data;
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError
  {
    return bytes;
  }

}