package com.linkedin.helix.store;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;

public class ZNRecordJsonSerializer implements PropertySerializer<ZNRecord>
{
  static private Logger LOG = Logger.getLogger(ZNRecordJsonSerializer.class);
  private final ZNRecordSerializer _serializer = new ZNRecordSerializer();
  
  @Override
  public byte[] serialize(ZNRecord data) throws PropertyStoreException
  {
    return _serializer.serialize(data);
  }

  @Override
  public ZNRecord deserialize(byte[] bytes) throws PropertyStoreException
  {
    return (ZNRecord) _serializer.deserialize(bytes);
  }

}
