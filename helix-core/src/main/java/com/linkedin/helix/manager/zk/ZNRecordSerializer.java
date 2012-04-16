package com.linkedin.helix.manager.zk;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.ZNRecord;

public class ZNRecordSerializer implements ZkSerializer
{
  private static Logger logger = Logger.getLogger(ZNRecordSerializer.class);

  @Override
  public byte[] serialize(Object data)
  {
    if (!(data instanceof ZNRecord))
    {
      logger.error("Input object must be of type ZNRecord but it is " + data);
      return new byte[] {};
    }

    ZNRecord record = (ZNRecord) data;
    ObjectMapper mapper = new ObjectMapper();

    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    StringWriter sw = new StringWriter();
    try
    {
      mapper.writeValue(sw, data);

      if (sw.toString().getBytes().length > ZNRecord.SIZE_LIMIT)
      {
        throw new HelixException("Data size larger than 1M. ZNRecord.id: " + record.getId());
      }
      return sw.toString().getBytes();
    } catch (Exception e)
    {
      logger.error("Exception during data serialization. Will not write to zk. Data (first 1k): "
          + sw.toString().substring(0, 1024), e);
      throw new HelixException(e);
      // return new byte[] {};
    }
  }

  @Override
  public Object deserialize(byte[] bytes)
  {
    if (bytes == null || bytes.length == 0)
    {
      logger.error("Znode is empty.");
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, true);
    deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    try
    {
      ZNRecord zn = mapper.readValue(bais, ZNRecord.class);
      return zn;
    } catch (Exception e)
    {
      logger.error("Exception during deserialization of bytes: " + new String(bytes), e);
      return null;
    }
  }
}
