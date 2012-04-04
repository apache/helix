package com.linkedin.helix.manager.zk;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.util.Map;
import java.util.TreeMap;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDelta;
import com.linkedin.helix.model.Message;

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
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS,
                            true);
    StringWriter sw = new StringWriter();
    try
    {
      mapper.writeValue(sw, data);

      if (sw.toString().getBytes().length > ZNRecord.SIZE_LIMIT)
      {
        throw new HelixException("Data size larger than 1M. Write empty string to zk. ZNRecord.id: "
            + record.getId());
      }
      return sw.toString().getBytes();
    }
    catch (Exception e)
    {
      logger.error("Error during serialization of data (first 1k): "
          + sw.toString().substring(0, 1024), e);
    }

    return new byte[] {};
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
    deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
                              true);
    try
    {
      ZNRecord zn = mapper.readValue(bais, ZNRecord.class);
      return zn;
    }
    catch (Exception e)
    {
      logger.error("Error during deserialization of bytes: " + new String(bytes), e);
    }

    return null;
  }

  public static void main(String[] args)
  {
    ZNRecord record = new ZNRecord("asdsa");
    Map<String, String> v = new TreeMap<String, String>();
    v.put("KEY!", "asdas");
    record.setSimpleField("asdsa", "adasdsdasd");
    // record.setSimpleField("asdsa", "adasdsdasd");
    record.setMapField("db.partion-0", v);
    ZNRecordDelta deltaRecord = new ZNRecordDelta(record);

    Message message = new Message(record);

    ZNRecordSerializer serializer = new ZNRecordSerializer();
    byte[] bytes;
    bytes = serializer.serialize(record);
    System.out.println(new String(bytes));

    // bytes = serializer.serialize(message);
    System.out.println(new String(bytes));

    ZNRecord newRecord = (ZNRecord) serializer.deserialize(bytes);
    System.out.println(newRecord);

    ZkClient client = new ZkClient("localhost:2181");
    client.setZkSerializer(serializer);
    Object readData =
        client.readData("/test-cluster/instances/localhost_8900/currentStates/test_DB.partition-2");
    System.out.println(readData);
  }

}
