package org.apache.helix.webapp.resources;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.LiveInstance.LiveInstanceProperty;
import org.apache.helix.util.HelixUtil;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.type.TypeReference;
import org.restlet.data.Form;
import org.restlet.data.MediaType;

public class ClusterRepresentationUtil {
  public static final ZNRecord EMPTY_ZNRECORD = new ZNRecord("EMPTY_ZNRECORD");

  public static String getClusterPropertyAsString(ZkClient zkClient, String clusterName,
      PropertyKey propertyKey,
      // String key,
      MediaType mediaType)

  throws JsonGenerationException, JsonMappingException, IOException {
    return getClusterPropertyAsString(zkClient, clusterName, mediaType, propertyKey);
  }

  public static String getClusterPropertyAsString(ZkClient zkClient, String clusterName,
      MediaType mediaType, PropertyKey propertyKey) throws JsonGenerationException,
      JsonMappingException, IOException {

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

    HelixProperty property = accessor.getProperty(propertyKey);
    ZNRecord record = property == null ? null : property.getRecord();
    return ZNRecordToJson(record);
  }

  public static String getInstancePropertyNameListAsString(ZkClient zkClient, String clusterName,
      String instanceName, PropertyType instanceProperty, String key, MediaType mediaType)
      throws JsonGenerationException, JsonMappingException, IOException {
    String path =
        HelixUtil.getInstancePropertyPath(clusterName, instanceName, instanceProperty) + "/" + key;
    if (zkClient.exists(path)) {
      List<String> recordNames = zkClient.getChildren(path);
      return ObjectToJson(recordNames);
    }

    return ObjectToJson(new ArrayList<String>());
  }

  public static String getInstancePropertyAsString(ZkClient zkClient, String clusterName,
      PropertyKey propertyKey, MediaType mediaType) throws JsonGenerationException,
      JsonMappingException, IOException {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

    ZNRecord records = accessor.getProperty(propertyKey).getRecord();
    return ZNRecordToJson(records);
  }

  public static String getInstancePropertiesAsString(ZkClient zkClient, String clusterName,
      PropertyKey propertyKey, MediaType mediaType) throws JsonGenerationException,
      JsonMappingException, IOException {
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

    List<ZNRecord> records = HelixProperty.convertToList(accessor.getChildValues(propertyKey));
    return ObjectToJson(records);
  }

  public static String getPropertyAsString(ZkClient zkClient, String clusterName,
      PropertyKey propertyKey, MediaType mediaType) throws JsonGenerationException,
      JsonMappingException, IOException {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

    ZNRecord record = EMPTY_ZNRECORD;
    HelixProperty property = accessor.getProperty(propertyKey);
    if (property != null) {
      record = property.getRecord();
    }
    return ObjectToJson(record);
  }

  public static String ZNRecordToJson(ZNRecord record) throws JsonGenerationException,
      JsonMappingException, IOException {
    return ObjectToJson(record);
  }

  public static String ObjectToJson(Object object) throws JsonGenerationException,
      JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, object);

    return sw.toString();
  }

  public static HelixDataAccessor getClusterDataAccessor(ZkClient zkClient, String clusterName) {
    return new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
  }

  public static <T extends Object> T JsonToObject(Class<T> clazz, String jsonString)
      throws JsonParseException, JsonMappingException, IOException {
    StringReader sr = new StringReader(jsonString);
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(sr, clazz);

  }

  public static Map<String, String> JsonToMap(String jsonString) throws JsonParseException,
      JsonMappingException, IOException {
    StringReader sr = new StringReader(jsonString);
    ObjectMapper mapper = new ObjectMapper();

    TypeReference<TreeMap<String, String>> typeRef = new TypeReference<TreeMap<String, String>>() {
    };

    return mapper.readValue(sr, typeRef);
  }

  public static Map<String, String> getFormJsonParameters(Form form) throws JsonParseException,
      JsonMappingException, IOException {
    String jsonPayload = form.getFirstValue(JsonParameters.JSON_PARAMETERS, true);
    return ClusterRepresentationUtil.JsonToMap(jsonPayload);
  }

  public static Map<String, String> getFormJsonParameters(Form form, String key)
      throws JsonParseException, JsonMappingException, IOException {
    String jsonPayload = form.getFirstValue(key, true);
    return ClusterRepresentationUtil.JsonToMap(jsonPayload);
  }

  public static String getFormJsonParameterString(Form form, String key) throws JsonParseException,
      JsonMappingException, IOException {
    return form.getFirstValue(key, true);
  }

  public static <T extends Object> T getFormJsonParameters(Class<T> clazz, Form form, String key)
      throws JsonParseException, JsonMappingException, IOException {
    return JsonToObject(clazz, form.getFirstValue(key, true));
  }

  public static String getErrorAsJsonStringFromException(Exception e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);

    String error = e.getMessage() + '\n' + sw.toString();
    Map<String, String> result = new TreeMap<String, String>();
    result.put("ERROR", error);
    try {
      return ObjectToJson(result);
    } catch (Exception e1) {
      StringWriter sw1 = new StringWriter();
      PrintWriter pw1 = new PrintWriter(sw1);
      e.printStackTrace(pw1);
      return "{\"ERROR\": \"" + sw1.toString() + "\"}";
    }
  }

  public static String getInstanceSessionId(ZkClient zkClient, String clusterName,
      String instanceName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName)).getRecord();

    return liveInstance.getSimpleField(LiveInstanceProperty.SESSION_ID.toString());
  }

  public static List<String> getInstancePropertyList(ZkClient zkClient, String clusterName,
      String instanceName, PropertyType property, String key) {
    String propertyPath =
        HelixUtil.getInstancePropertyPath(clusterName, instanceName, property) + "/" + key;

    return zkClient.getChildren(propertyPath);

  }
}
