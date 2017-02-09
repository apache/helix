package org.apache.helix.util;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.PropertyType;
import org.apache.log4j.Logger;

public final class HelixUtil {
  static private Logger LOG = Logger.getLogger(HelixUtil.class);

  private HelixUtil() {
  }

  public static String getInstanceNameFromPath(String path) {
    // path structure
    // /<cluster_name>/instances/<instance_name>/[currentStates/messages]
    if (path.contains("/" + PropertyType.INSTANCES + "/")) {
      String[] split = path.split("\\/");
      if (split.length > 3) {
        return split[3];
      }
    }
    return null;
  }

  /**
   * get the parent-path of given path
   * return "/" string if path = "/xxx", null if path = "/"
   * @param path
   * @return
   */
  public static String getZkParentPath(String path) {
    if (path.equals("/")) {
      return null;
    }

    int idx = path.lastIndexOf('/');
    return idx == 0 ? "/" : path.substring(0, idx);
  }

  /**
   * get the last part of the zk-path
   * @param path
   * @return
   */
  public static String getZkName(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  public static String serializeByComma(List<String> objects) {
    return String.join(",", objects);
  }

  public static List<String> deserializeByComma(String object) {
    if (object.length() == 0) {
      return Collections.EMPTY_LIST;
    }
    return Arrays.asList(object.split(","));
  }

  /**
   * parse a csv-formated key-value pairs
   * @param keyValuePairs : csv-formatted key-value pairs. e.g. k1=v1,k2=v2,...
   * @return
   */
  public static Map<String, String> parseCsvFormatedKeyValuePairs(String keyValuePairs) {
    String[] pairs = keyValuePairs.split("[\\s,]");
    Map<String, String> keyValueMap = new TreeMap<String, String>();
    for (String pair : pairs) {
      int idx = pair.indexOf('=');
      if (idx == -1) {
        LOG.error("Invalid key-value pair: " + pair + ". Igonore it.");
        continue;
      }

      String key = pair.substring(0, idx);
      String value = pair.substring(idx + 1);
      keyValueMap.put(key, value);
    }
    return keyValueMap;
  }

  /**
   * Attempts to load the class and delegates to TCCL if class is not found.
   * Note: The approach is used as a last resort for environments like OSGi.
   * @param className
   * @return
   * @throws ClassNotFoundException
   */
  public static <T> Class<?> loadClass(Class<T> clazz, String className)
      throws ClassNotFoundException {
    try {
      return clazz.getClassLoader().loadClass(className);
    } catch (ClassNotFoundException ex) {
      if (Thread.currentThread().getContextClassLoader() != null) {
        return Thread.currentThread().getContextClassLoader().loadClass(className);
      } else {
        throw ex;
      }
    }
  }
}
