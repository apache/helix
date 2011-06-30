package com.linkedin.clustermanager.tools;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import com.linkedin.clustermanager.ClusterView;
import com.linkedin.clustermanager.agent.file.FileBasedClusterManager;

public class ClusterViewSerializer
{
  private static Logger logger = Logger.getLogger(ClusterViewSerializer.class);

  private File _file;

  public ClusterViewSerializer(String filename)
  {
    _file = new File(filename);
  }

  public byte[] serialize(Object data) 
  {
    ObjectMapper mapper = new ObjectMapper();

    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_GETTERS, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    // serializationConfig.set(SerializationConfig.Feature.WRITE_NULL_PROPERTIES,
    // true);

    StringWriter sw = new StringWriter();

    try
    {
      mapper.writeValue(_file, data);
      mapper.writeValue(sw, data);
      return sw.toString().getBytes();
    } catch (Exception e)
    {
      logger.error("Error during serialization of data:" + data, e);
    }

    return new byte[0];
  }

  public Object deserialize(byte[] bytes)
  {
    if (!_file.exists())
    {
      logger.error(String.format("Static config file \"%s\" doesn't exist", _file.getAbsolutePath()));
      return null; // DONT CHECK THIS IN // System.exit(1);
    }

    ObjectMapper mapper = new ObjectMapper();
    // ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, true);
    // deserializationConfig.set(DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS,
    // true);
    deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    try
    {
      // ClusterView view = mapper.readValue(bais, ClusterView.class);
      ClusterView view = mapper.readValue(_file, ClusterView.class);
      return view;
    } catch (Exception e)
    {
      logger.error("Error during deserialization of bytes:" + new String(bytes), e);
    }

    return null;
  }

  public static void main(String[] args) throws JsonGenerationException, JsonMappingException,
      IOException
  {
    // temporary test only
    // create fake db names
    List<FileBasedClusterManager.DBParam> dbParams = new ArrayList<FileBasedClusterManager.DBParam>();
    // dbParams.add(new FileBasedClusterManager.DBParam("BizFollow", 1));
    dbParams.add(new FileBasedClusterManager.DBParam("BizProfile", 1));
    // dbParams.add(new FileBasedClusterManager.DBParam("EspressoDB", 10));
    // dbParams.add(new FileBasedClusterManager.DBParam("MailboxDB", 128));
    // dbParams.add(new FileBasedClusterManager.DBParam("MyDB", 8));
    // dbParams.add(new FileBasedClusterManager.DBParam("schemata", 1));
    // String[] nodesInfo = { "localhost:8900", "localhost:8901",
    // "localhost:8902", "localhost:8903",
    // "localhost:8904" };
    String[] nodesInfo = { "localhost:12918" };
    int replication = 0;

    ClusterView view = FileBasedClusterManager.generateStaticConfigClusterView(nodesInfo, dbParams,
        replication);
    String file = "/tmp/cluster-view.json";
    ClusterViewSerializer serializer = new ClusterViewSerializer(file);

    byte[] bytes;
    bytes = serializer.serialize(view);
    logger.info("bytes=" + new String(bytes));

    ClusterView restoredView = (ClusterView) serializer.deserialize(bytes);
    // logger.info(restoredView);

    bytes = serializer.serialize(restoredView);
    // logger.info(new String(bytes));
  }
}
