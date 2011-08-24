package com.linkedin.clustermanagement.webapp.resources;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.type.TypeReference;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.resource.Representation;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.util.ZKClientPool;

public class ClusterRepresentationUtil
{
  public static final String _jsonParameters = "jsonParameters";
  public static final String _managementCommand = "command";
  public static final String _addInstanceCommand = "addInstance";
  public static final String _addHostedEntityCommand = "addHostedEntity";
  public static final String _addStateModelCommand = "addStateModel";
  public static final String _rebalanceCommand = "rebalance";
  public static final String _alterIdealStateCommand = "alterIdealState";
  public static final String _enableInstanceCommand = "enableInstance";
  public static final String _addClusterCommand = "addCluster";
  public static final String _alterStateModelCommand = "alterStateModel";
  public static final String _newIdealState = "newIdealState";
  public static final String _newModelDef = "newStateModelDef";
  public static final String _enabled = "enabled";
  
  public static String getClusterPropertyAsString(String zkServer, String clusterName, ClusterPropertyType clusterProperty, String key, MediaType mediaType) throws JsonGenerationException, JsonMappingException, IOException
  {
    ZkClient zkClient = ZKClientPool.getZkClient(zkServer);
    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
    
    ZNRecord record = accessor.getClusterProperty(clusterProperty, key);    
    return ZNRecordToJson(record);
  }
  
  public static String getInstancePropertyAsString(String zkServer, String clusterName, ClusterPropertyType clusterProperty, String key, MediaType mediaType) throws JsonGenerationException, JsonMappingException, IOException
  {
    ZkClient zkClient = ZKClientPool.getZkClient(zkServer);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
    
    ZNRecord record = accessor.getClusterProperty(clusterProperty, key);    
    return ZNRecordToJson(record);
  }
  
  public static String ZNRecordToJson(ZNRecord record) throws JsonGenerationException, JsonMappingException, IOException
  {
    return ObjectToJson(record);
  }
  
  public static String ObjectToJson(Object object) throws JsonGenerationException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, object);
    
    return sw.toString();
  }
  
  public static ClusterDataAccessor getClusterDataAccessor( String zkServer, String clusterName)
  {
    ZkClient zkClient = ZKClientPool.getZkClient(zkServer);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    return new ZKDataAccessor(clusterName, zkClient);
  }
  
  public static Map<String, String> JsonToMap(String jsonString) throws JsonParseException, JsonMappingException, IOException
  {
    StringReader sr = new StringReader(jsonString);
    ObjectMapper mapper = new ObjectMapper();
    
    TypeReference<TreeMap<String, String>> typeRef 
    = new TypeReference< 
           TreeMap<String,String> 
         >() {}; 

    return mapper.readValue(sr, typeRef);
  }
  
  public static Map<String, String> getFormJsonParameters(Form form) throws JsonParseException, JsonMappingException, IOException
  {
    String jsonPayload = form.getFirstValue(_jsonParameters, true);
    return  ClusterRepresentationUtil.JsonToMap(jsonPayload);
  }
  
  public static Map<String, String> getFormJsonParametersWithCommandVerified(Form form, String commandValue) throws JsonParseException, JsonMappingException, IOException
  {
    String jsonPayload = form.getFirstValue(_jsonParameters, true);
    if(jsonPayload == null || jsonPayload.isEmpty())
    {
      throw new ClusterManagerException("'"+_jsonParameters+"' in the POST body is empty");
    }
    Map<String, String> paraMap = ClusterRepresentationUtil.JsonToMap(jsonPayload);
    if(!paraMap.containsKey(_managementCommand))
    {
      throw new ClusterManagerException("Missing management paramater '"+_managementCommand +"'");
    }
    if(!paraMap.get(_managementCommand).equalsIgnoreCase(commandValue))
    {
      throw new ClusterManagerException(_managementCommand +" must be '"+commandValue +"'");
    }
    return paraMap;
  }
  
  public static String getErrorAsJsonStringFromException(Exception e)
  {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
   
    String error = e.getMessage() + "\n" + sw.toString();
    Map<String, String> result = new TreeMap<String, String>();
    result.put("ERROR", error);
    try
    {
      return ObjectToJson(result);
    } catch (Exception e1)
    {
      StringWriter sw1 = new StringWriter();
      PrintWriter pw1 = new PrintWriter(sw1);
      e.printStackTrace(pw1);
      return "{\"ERROR\": \"" + sw1.toString()+"\"}";
    } 
  }
}
