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
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.webapp.RestAdminApplication;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.data.Form;

public class ResourceUtil {
  private static final String EMPTY_ZNRECORD_STRING =
      objectToJson(ClusterRepresentationUtil.EMPTY_ZNRECORD);

  /**
   * Key enums for getting values from request
   */
  public enum RequestKey {
    CLUSTER_NAME("clusterName"),
    JOB_QUEUE("jobQueue"),
    JOB("job"),
    CONSTRAINT_TYPE("constraintType"),
    CONSTRAINT_ID("constraintId"),
    RESOURCE_NAME("resourceName"),
    INSTANCE_NAME("instanceName");

    private final String _key;

    RequestKey(String key) {
      _key = key;
    }

    public String toString() {
      return _key;
    }
  }

  /**
   * Key enums for getting values from context
   */
  public enum ContextKey {
    ZK_ADDR(RestAdminApplication.ZKSERVERADDRESS),
    ZKCLIENT(RestAdminApplication.ZKCLIENT),
    RAW_ZKCLIENT("rawZkClient"); // zkclient that uses raw-byte serializer

    private final String _key;

    ContextKey(String key) {
      _key = key;
    }

    public String toString() {
      return _key;
    }
  }

  /**
   * Key enums for getting yaml format parameters
   */
  public enum YamlParamKey {
    NEW_JOB("newJob");

    private final String _key;

    YamlParamKey(String key) {
      _key = key;
    }

    public String toString() {
      return _key;
    }
  }

  private static String objectToJson(Object object) {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    try {
      mapper.writeValue(sw, object);
    } catch (JsonGenerationException e) {
      // Should not be here
    } catch (JsonMappingException e) {
      // Should not be here
    } catch (IOException e) {
      // Should not be here
    }

    return sw.toString();
  }

  public static String getAttributeFromRequest(Request r, RequestKey key) {
    return (String) r.getAttributes().get(key.toString());
  }

  public static ZkClient getAttributeFromCtx(Context ctx, ContextKey key) {
    return (ZkClient) ctx.getAttributes().get(key.toString());
  }

  @SuppressWarnings("unchecked")
  public static <T> T getTypedAttributeFromCtx(Class<T> klass, Context ctx, ContextKey key) {
    return (T) ctx.getAttributes().get(key.toString());
  }

  public static String getYamlParameters(Form form, YamlParamKey key) {
    return form.getFirstValue(key.toString());
  }

  public static String readZkAsBytes(ZkClient zkclient, PropertyKey propertyKey) {
    byte[] bytes = zkclient.readData(propertyKey.getPath());
    return bytes == null ? EMPTY_ZNRECORD_STRING : new String(bytes);
  }

  static String extractSimpleFieldFromZNRecord(String recordStr, String key) {
    int idx = recordStr.indexOf(key);
    if (idx != -1) {
      idx = recordStr.indexOf('"', idx + key.length() + 1);
      if (idx != -1) {
        int idx2 = recordStr.indexOf('"', idx + 1);
        if (idx2 != -1) {
          return recordStr.substring(idx + 1, idx2);
        }
      }

    }
    return null;
  }

  public static Map<String, String> readZkChildrenAsBytesMap(ZkClient zkclient,
      PropertyKey propertyKey) {
    BaseDataAccessor<byte[]> baseAccessor = new ZkBaseDataAccessor<byte[]>(zkclient);
    String parentPath = propertyKey.getPath();
    List<String> childNames = baseAccessor.getChildNames(parentPath, 0);
    if (childNames == null) {
      return null;
    }
    List<String> paths = new ArrayList<String>();
    for (String childName : childNames) {
      paths.add(parentPath + "/" + childName);
    }
    List<byte[]> values = baseAccessor.get(paths, null, 0);
    Map<String, String> ret = new HashMap<String, String>();
    for (int i = 0; i < childNames.size(); i++) {
      ret.put(childNames.get(i), new String(values.get(i)));
    }
    return ret;
  }
}
