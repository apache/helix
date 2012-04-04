package com.linkedin.helix.store;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.ZNRecord;

public class ZNRecordJsonSerializer implements PropertySerializer<ZNRecord>
{
  static private Logger LOG = Logger.getLogger(ZNRecordJsonSerializer.class);

  private int getListLengthBound(ZNRecord record)
  {
    int max = Integer.MAX_VALUE;
    if (record.getSimpleFields().containsKey(ZNRecord.LIST_FIELD_BOUND))
    {
      String maxStr = record.getSimpleField(ZNRecord.LIST_FIELD_BOUND);
      try
      {
        max = Integer.parseInt(maxStr);
      }
      catch (Exception e)
      {
        LOG.error("IllegalNumberFormat for list length bound: " + maxStr);
      }
    }
    return max;
  }

  @Override
  public byte[] serialize(ZNRecord data) throws PropertyStoreException
  {
    // apply retention policy
    int max = getListLengthBound(data);
    if (max < Integer.MAX_VALUE)
    {
      Map<String, List<String>> listMap = data.getListFields();
      for (String key : listMap.keySet())
      {
        List<String> list = listMap.get(key);
        if (list.size() > max)
        {
          listMap.put(key, list.subList(0, max));
        }
      }
    }

    // do serialization
    ObjectMapper mapper = new ObjectMapper();

    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS,
                            true);
    StringWriter sw = new StringWriter();

    try
    {
      mapper.writeValue(sw, data);

      if (sw.toString().getBytes().length > ZNRecord.SIZE_LIMIT)
      {
        throw new HelixException("Data size larger than 1M. Write empty string to zk. id: "
            + data.getId());
      }
      return sw.toString().getBytes();
    }
    catch (Exception e)
    {
      LOG.error("Error during serialization of data (first 1k): "
          + sw.toString().substring(0, 1024), e);
    }

    return new byte[] {};
  }

  @Override
  public ZNRecord deserialize(byte[] bytes) throws PropertyStoreException
  {
    if (bytes == null || bytes.length == 0)
    {
      LOG.error("Znode is empty.");
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, true);
    deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
                              true);
    try
    {
      ZNRecord value = mapper.readValue(bais, ZNRecord.class);
      return value;
    }
    catch (Exception e)
    {
      LOG.error("Error during deserialization of bytes: " + new String(bytes), e);
    }

    return null;
  }

}
