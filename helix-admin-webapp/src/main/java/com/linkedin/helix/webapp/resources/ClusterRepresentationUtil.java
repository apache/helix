/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.webapp.resources;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
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

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;
import com.linkedin.helix.util.HelixUtil;
import com.linkedin.helix.webapp.RestAdminApplication;

public class ClusterRepresentationUtil
{
  public static final String _jsonParameters                        = "jsonParameters";
  public static final String _managementCommand                     = "command";
  public static final String _newIdealState                         = "newIdealState";
  public static final String _newModelDef                           = "newStateModelDef";
  public static final String _enabled                               = "enabled";
  

  // public static String getClusterPropertyAsString(String zkServer, String clusterName,
  // PropertyType clusterProperty, String key, MediaType mediaType)
  public static String getClusterPropertyAsString(
                                                  String clusterName,
                                                  PropertyKey propertyKey,
                                                  // String key,
                                                  MediaType mediaType)

  throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    return getClusterPropertyAsString(clusterName, mediaType, propertyKey);
  }

  // public static String getClusterPropertyAsString(String zkServer, String clusterName,
  // MediaType mediaType, PropertyType clusterProperty, String... keys)
  public static String getClusterPropertyAsString(
                                                  String clusterName,
                                                  MediaType mediaType,
                                                  PropertyKey propertyKey) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    ZkClient zkClient = RestAdminApplication.getZkClient();

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));

    ZNRecord record = accessor.getProperty(propertyKey).getRecord();
    return ZNRecordToJson(record);
  }

  // public static String getInstancePropertyListAsString(String zkServer,
  // String clusterName,
  // String instanceName,
  // PropertyType instanceProperty,
  // String key,
  // MediaType mediaType) throws JsonGenerationException,
  // JsonMappingException,
  // IOException
  // {
  // ZkClient zkClient = ZKClientPool.getZkClient(zkServer);
  // zkClient.setZkSerializer(new ZNRecordSerializer());
  //
  // String path =
  // HelixUtil.getInstancePropertyPath(clusterName, instanceName, instanceProperty)
  // + "/" + key;
  // if (zkClient.exists(path))
  // {
  // DataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
  // List<ZNRecord> records =
  // accessor.getChildValues(instanceProperty, instanceName, key);
  // return ObjectToJson(records);
  // }
  //
  // return ObjectToJson(new ArrayList<ZNRecord>());
  // }

  public static String getInstancePropertyNameListAsString(
                                                           String clusterName,
                                                           String instanceName,
                                                           PropertyType instanceProperty,
                                                           String key,
                                                           MediaType mediaType) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    ZkClient zkClient = RestAdminApplication.getZkClient();

    String path =
        HelixUtil.getInstancePropertyPath(clusterName, instanceName, instanceProperty)
            + "/" + key;
    if (zkClient.exists(path))
    {
      List<String> recordNames = zkClient.getChildren(path);
      return ObjectToJson(recordNames);
    }

    return ObjectToJson(new ArrayList<String>());
  }

  public static String getInstancePropertyAsString(
                                                   String clusterName,
                                                   PropertyKey propertyKey,
                                                   MediaType mediaType) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    ZkClient zkClient = RestAdminApplication.getZkClient();
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));

    ZNRecord records = accessor.getProperty(propertyKey).getRecord();
    return ZNRecordToJson(records);
  }

  public static String getInstancePropertiesAsString(
                                                     String clusterName,
                                                     PropertyKey propertyKey,
                                                     MediaType mediaType) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    ZkClient zkClient = RestAdminApplication.getZkClient();
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));

    List<ZNRecord> records =
        HelixProperty.convertToList(accessor.getChildValues(propertyKey));
    return ObjectToJson(records);
  }

  public static String getPropertyAsString(
                                           String clusterName,
                                           PropertyKey propertyKey,
                                           MediaType mediaType) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    ZkClient zkClient = RestAdminApplication.getZkClient();
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));

    ZNRecord record = accessor.getProperty(propertyKey).getRecord();
    return ObjectToJson(record);
  }

  public static String ZNRecordToJson(ZNRecord record) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    return ObjectToJson(record);
  }

  public static String ObjectToJson(Object object) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, object);

    return sw.toString();
  }

  public static HelixDataAccessor getClusterDataAccessor( String clusterName)
  {
    ZkClient zkClient = RestAdminApplication.getZkClient();
    return new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));
  }

  public static <T extends Object> T JsonToObject(Class<T> clazz, String jsonString) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    StringReader sr = new StringReader(jsonString);
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(sr, clazz);

  }

  public static Map<String, String> JsonToMap(String jsonString) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    StringReader sr = new StringReader(jsonString);
    ObjectMapper mapper = new ObjectMapper();

    TypeReference<TreeMap<String, String>> typeRef =
        new TypeReference<TreeMap<String, String>>()
        {
        };

    return mapper.readValue(sr, typeRef);
  }

  public static Map<String, String> getFormJsonParameters(Form form) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    String jsonPayload = form.getFirstValue(_jsonParameters, true);
    return ClusterRepresentationUtil.JsonToMap(jsonPayload);
  }

  public static Map<String, String> getFormJsonParameters(Form form, String key) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    String jsonPayload = form.getFirstValue(key, true);
    return ClusterRepresentationUtil.JsonToMap(jsonPayload);
  }

  public static String getFormJsonParameterString(Form form, String key) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    return form.getFirstValue(key, true);
  }

  public static <T extends Object> T getFormJsonParameters(Class<T> clazz,
                                                           Form form,
                                                           String key) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    return JsonToObject(clazz, form.getFirstValue(key, true));
  }

  public static Map<String, String> getFormJsonParametersWithCommandVerified(Form form,
                                                                             String commandValue) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    String jsonPayload = form.getFirstValue(_jsonParameters, true);
    if (jsonPayload == null || jsonPayload.isEmpty())
    {
      throw new HelixException("'" + _jsonParameters + "' in the POST body is empty");
    }
    Map<String, String> paraMap = ClusterRepresentationUtil.JsonToMap(jsonPayload);
    if (!paraMap.containsKey(_managementCommand))
    {
      throw new HelixException("Missing management paramater '" + _managementCommand
          + "'");
    }
    if (!paraMap.get(_managementCommand).equalsIgnoreCase(commandValue))
    {
      throw new HelixException(_managementCommand + " must be '" + commandValue + "'");
    }
    return paraMap;
  }

  public static String getErrorAsJsonStringFromException(Exception e)
  {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);

    String error = e.getMessage() + '\n' + sw.toString();
    Map<String, String> result = new TreeMap<String, String>();
    result.put("ERROR", error);
    try
    {
      return ObjectToJson(result);
    }
    catch (Exception e1)
    {
      StringWriter sw1 = new StringWriter();
      PrintWriter pw1 = new PrintWriter(sw1);
      e.printStackTrace(pw1);
      return "{\"ERROR\": \"" + sw1.toString() + "\"}";
    }
  }

  public static String getInstanceSessionId(
                                            String clusterName,
                                            String instanceName)
  {
    ZkClient zkClient = RestAdminApplication.getZkClient();
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord liveInstance =
        accessor.getProperty(keyBuilder.liveInstance(instanceName)).getRecord();

    return liveInstance.getSimpleField(LiveInstanceProperty.SESSION_ID.toString());
  }

  public static List<String> getInstancePropertyList(
                                                     String clusterName,
                                                     String instanceName,
                                                     PropertyType property,
                                                     String key)
  {
    ZkClient zkClient = RestAdminApplication.getZkClient();
    String propertyPath =
        HelixUtil.getInstancePropertyPath(clusterName, instanceName, property) + "/"
            + key;

    return zkClient.getChildren(propertyPath);

  }
}
