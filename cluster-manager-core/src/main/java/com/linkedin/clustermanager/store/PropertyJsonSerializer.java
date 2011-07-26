package com.linkedin.clustermanager.store;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class PropertyJsonSerializer<T> implements PropertySerializer<T>
{
  static private Logger LOG = Logger.getLogger(PropertyJsonSerializer.class);
  private final Class<T> _clazz;
  
  public PropertyJsonSerializer(Class<T> clazz)
  {
    _clazz = clazz;
  }

  @Override
  public byte[] serialize(T data) throws PropertyStoreException
  {
    ObjectMapper mapper = new ObjectMapper();

    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    StringWriter sw = new StringWriter();
    
    try
    {
      mapper.writeValue(sw, data);
      return sw.toString().getBytes();
    } 
    catch (Exception e)
    {
      LOG.error("Error during serialization of data:" + data, e);
    }

    return null;
  }

  @Override
  public T deserialize(byte[] bytes) throws PropertyStoreException
  {
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, true);
    deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    try
    {
      T value = mapper.readValue(bais, _clazz);
      return value;
    } 
    catch (Exception e)
    {
      LOG.error("Error during deserialization of bytes:" + new String(bytes), e);
    }

    return null;
  }
  

}
