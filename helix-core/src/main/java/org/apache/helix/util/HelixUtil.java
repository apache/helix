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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.log4j.Logger;

public final class HelixUtil {
  static private Logger LOG = Logger.getLogger(HelixUtil.class);

  private HelixUtil() {
  }

  public static String getPropertyPath(String clusterName, PropertyType type) {
    return "/" + clusterName + "/" + type.toString();
  }

  public static String getInstancePropertyPath(String clusterName, String instanceName,
      PropertyType type) {
    return getPropertyPath(clusterName, PropertyType.INSTANCES) + "/" + instanceName + "/"
        + type.toString();
  }

  public static String getInstancePath(String clusterName, String instanceName) {
    return getPropertyPath(clusterName, PropertyType.INSTANCES) + "/" + instanceName;
  }

  public static String getIdealStatePath(String clusterName, String resourceName) {
    return getPropertyPath(clusterName, PropertyType.IDEALSTATES) + "/" + resourceName;
  }

  public static String getIdealStatePath(String clusterName) {
    return getPropertyPath(clusterName, PropertyType.IDEALSTATES);
  }

  public static String getLiveInstancesPath(String clusterName) {
    return getPropertyPath(clusterName, PropertyType.LIVEINSTANCES);
  }

  public static String getMessagePath(String clusterName, String instanceName) {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.MESSAGES);
  }

  public static String getCurrentStateBasePath(String clusterName, String instanceName) {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.CURRENTSTATES);
  }

  /**
   * Even though this is simple we want to have the mechanism of bucketing the
   * partitions. If we have P partitions and N nodes with K replication factor
   * and D databases. Then on each node we will have (P/N)*K*D partitions. And
   * cluster manager neeeds to maintain watch on each of these nodes for every
   * node. So over all cluster manager will have P*K*D watches which can be
   * quite large given that we over partition.
   * The other extreme is having one znode per storage per database. This will
   * result in N*D watches which is good. But data in every node might become
   * really big since it has to save partition
   * Ideally we want to balance between the two models
   */
  public static String getCurrentStatePath(String clusterName, String instanceName,
      String sessionId, String stateUnitKey) {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.CURRENTSTATES) + "/"
        + sessionId + "/" + stateUnitKey;
  }

  public static String getExternalViewPath(String clusterName) {
    return getPropertyPath(clusterName, PropertyType.EXTERNALVIEW);
  }

  public static String getStateModelDefinitionPath(String clusterName) {
    return getPropertyPath(clusterName, PropertyType.STATEMODELDEFS);
  }

  public static String getExternalViewPath(String clusterName, String resourceName) {
    return getPropertyPath(clusterName, PropertyType.EXTERNALVIEW) + "/" + resourceName;
  }

  public static String getLiveInstancePath(String clusterName, String instanceName) {
    return getPropertyPath(clusterName, PropertyType.LIVEINSTANCES) + "/" + instanceName;
  }

  public static String getMemberInstancesPath(String clusterName) {
    return getPropertyPath(clusterName, PropertyType.INSTANCES);
  }

  public static String getErrorsPath(String clusterName, String instanceName) {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.ERRORS);
  }

  public static String getStatusUpdatesPath(String clusterName, String instanceName) {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.STATUSUPDATES);
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

  // distributed cluster controller
  public static String getControllerPath(String clusterName) {
    return getPropertyPath(clusterName, PropertyType.CONTROLLER);
  }

  public static String getControllerPropertyPath(String clusterName, PropertyType type) {
    return PropertyPathConfig.getPath(type, clusterName);
  }

  /**
   * Get the required paths for a valid cluster
   * @param clusterName the cluster to check
   * @return List of paths as strings
   */
  public static List<String> getRequiredPathsForCluster(String clusterName) {
    List<String> requiredPaths = new ArrayList<String>();
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
        ConfigScopeProperty.CLUSTER.toString(), clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
        ConfigScopeProperty.PARTICIPANT.toString()));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
        ConfigScopeProperty.RESOURCE.toString()));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.LIVEINSTANCES, clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.INSTANCES, clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.CONTROLLER, clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS, clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES_CONTROLLER, clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.ERRORS_CONTROLLER, clusterName));
    requiredPaths.add(PropertyPathConfig
        .getPath(PropertyType.STATUSUPDATES_CONTROLLER, clusterName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.HISTORY, clusterName));
    return requiredPaths;
  }

  /**
   * Get the required paths for a valid instance
   * @param clusterName the cluster that owns the instance
   * @param instanceName the instance to check
   * @return List of paths as strings
   */
  public static List<String> getRequiredPathsForInstance(String clusterName, String instanceName) {
    List<String> requiredPaths = new ArrayList<String>();
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
        ConfigScopeProperty.PARTICIPANT.toString(), instanceName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, clusterName, instanceName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName,
        instanceName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.STATUSUPDATES, clusterName,
        instanceName));
    requiredPaths.add(PropertyPathConfig.getPath(PropertyType.ERRORS, clusterName, instanceName));
    return requiredPaths;
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
