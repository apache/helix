package com.linkedin.clustermanagement.webapp.resources;

import java.io.IOException;
import java.io.StringWriter;

import org.I0Itec.zkclient.ZkClient;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.restlet.data.MediaType;
import org.restlet.resource.Representation;

import com.linkedin.clustermanager.core.ClusterDataAccessor;
import com.linkedin.clustermanager.core.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.impl.zk.ZKDataAccessor;
import com.linkedin.clustermanager.impl.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.model.ZNRecord;

public class ClusterRepresentationUtil
{
  public static String getClusterPropertyAsString(String zkServer, String clusterName, ClusterPropertyType clusterProperty, String key, MediaType mediaType) throws JsonGenerationException, JsonMappingException, IOException
  {
    ZkClient zkClient = new ZkClient(zkServer);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
    
    ZNRecord record = accessor.getClusterProperty(clusterProperty, key);    
    return ZNRecordToJson(record);
  }
  
  public static String getInstancePropertyAsString(String zkServer, String clusterName, ClusterPropertyType clusterProperty, String key, MediaType mediaType) throws JsonGenerationException, JsonMappingException, IOException
  {
    ZkClient zkClient = new ZkClient(zkServer);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
    
    ZNRecord record = accessor.getClusterProperty(clusterProperty, key);    
    return ZNRecordToJson(record);
  }
  
  static String ZNRecordToJson(ZNRecord record) throws JsonGenerationException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, record);
    
    return sw.toString();
  }
  
  public static ClusterDataAccessor getClusterDataAccessor( String zkServer, String clusterName)
  {
    ZkClient zkClient = new ZkClient(zkServer);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    return new ZKDataAccessor(clusterName, zkClient);
  }
}
