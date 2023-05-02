package org.apache.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.helix.model.ClusterStatus;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.StatusUpdate;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility mapping properties to their Zookeeper locations
 */
public class PropertyPathBuilder {
  private static Logger logger = LoggerFactory.getLogger(PropertyPathBuilder.class);

  static final Map<PropertyType, Map<Integer, String>> templateMap =
      new HashMap<PropertyType, Map<Integer, String>>();
  @Deprecated // typeToClassMapping is not being used anywhere
  static final Map<PropertyType, Class<? extends HelixProperty>> typeToClassMapping =
      new HashMap<PropertyType, Class<? extends HelixProperty>>();
  static {
    typeToClassMapping.put(PropertyType.LIVEINSTANCES, LiveInstance.class);
    typeToClassMapping.put(PropertyType.IDEALSTATES, IdealState.class);
    typeToClassMapping.put(PropertyType.CONFIGS, InstanceConfig.class);
    typeToClassMapping.put(PropertyType.EXTERNALVIEW, ExternalView.class);
    typeToClassMapping.put(PropertyType.CUSTOMIZEDVIEW, CustomizedView.class);
    typeToClassMapping.put(PropertyType.STATEMODELDEFS, StateModelDefinition.class);
    typeToClassMapping.put(PropertyType.MESSAGES, Message.class);
    typeToClassMapping.put(PropertyType.CURRENTSTATES, CurrentState.class);
    typeToClassMapping.put(PropertyType.STATUSUPDATES, StatusUpdate.class);
    typeToClassMapping.put(PropertyType.HISTORY, ControllerHistory.class);
    typeToClassMapping.put(PropertyType.PAUSE, PauseSignal.class);
    typeToClassMapping.put(PropertyType.MAINTENANCE, MaintenanceSignal.class);
    typeToClassMapping.put(PropertyType.STATUS, ClusterStatus.class);
    // TODO: Below must handle the case for future versions of Task Framework with a different path
    // structure
    typeToClassMapping.put(PropertyType.WORKFLOWCONTEXT, WorkflowContext.class);

    // @formatter:off
    addEntry(PropertyType.CONFIGS, 1, "/{clusterName}/CONFIGS");
    addEntry(PropertyType.CONFIGS, 2, "/{clusterName}/CONFIGS/{scope}");
    addEntry(PropertyType.CONFIGS, 3, "/{clusterName}/CONFIGS/{scope}/{scopeKey}");
    // addEntry(PropertyType.CONFIGS,2,"/{clusterName}/CONFIGS/{instanceName}");
    addEntry(PropertyType.LIVEINSTANCES, 1, "/{clusterName}/LIVEINSTANCES");
    addEntry(PropertyType.LIVEINSTANCES, 2, "/{clusterName}/LIVEINSTANCES/{instanceName}");
    addEntry(PropertyType.INSTANCES, 1, "/{clusterName}/INSTANCES");
    addEntry(PropertyType.INSTANCES, 2, "/{clusterName}/INSTANCES/{instanceName}");
    addEntry(PropertyType.IDEALSTATES, 1, "/{clusterName}/IDEALSTATES");
    addEntry(PropertyType.IDEALSTATES, 2, "/{clusterName}/IDEALSTATES/{resourceName}");
    addEntry(PropertyType.EXTERNALVIEW, 1, "/{clusterName}/EXTERNALVIEW");
    addEntry(PropertyType.EXTERNALVIEW, 2, "/{clusterName}/EXTERNALVIEW/{resourceName}");
    addEntry(PropertyType.CUSTOMIZEDVIEW, 1, "/{clusterName}/CUSTOMIZEDVIEW");
    addEntry(PropertyType.CUSTOMIZEDVIEW, 2, "/{clusterName}/CUSTOMIZEDVIEW/{customizedStateType}");
    addEntry(PropertyType.CUSTOMIZEDVIEW, 3, "/{clusterName}/CUSTOMIZEDVIEW/{customizedStateType}/{resourceName}");
    addEntry(PropertyType.STATUS, 1, "/{clusterName}/STATUS");
    addEntry(PropertyType.STATUS, 2, "/{clusterName}/STATUS/{clusterName}");

    addEntry(PropertyType.TARGETEXTERNALVIEW, 1, "/{clusterName}/TARGETEXTERNALVIEW");
    addEntry(PropertyType.TARGETEXTERNALVIEW, 2,
        "/{clusterName}/TARGETEXTERNALVIEW/{resourceName}");
    addEntry(PropertyType.CUSTOMIZEDVIEW, 1, "/{clusterName}/CUSTOMIZEDVIEW");
    addEntry(PropertyType.CUSTOMIZEDVIEW, 2, "/{clusterName}/CUSTOMIZEDVIEW/{resourceName}");
    addEntry(PropertyType.CUSTOMIZEDVIEW, 3,
        "/{clusterName}/CUSTOMIZEDVIEW/{resourceName}/{customizedStateName}");
    addEntry(PropertyType.STATEMODELDEFS, 1, "/{clusterName}/STATEMODELDEFS");
    addEntry(PropertyType.STATEMODELDEFS, 2, "/{clusterName}/STATEMODELDEFS/{stateModelName}");
    addEntry(PropertyType.CONTROLLER, 1, "/{clusterName}/CONTROLLER");
    addEntry(PropertyType.PROPERTYSTORE, 1, "/{clusterName}/PROPERTYSTORE");

    // INSTANCE
    addEntry(PropertyType.MESSAGES, 2, "/{clusterName}/INSTANCES/{instanceName}/MESSAGES");
    addEntry(PropertyType.MESSAGES, 3, "/{clusterName}/INSTANCES/{instanceName}/MESSAGES/{msgId}");
    addEntry(PropertyType.CURRENTSTATES, 2,
        "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES");
    addEntry(PropertyType.CURRENTSTATES, 3,
        "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}");
    addEntry(PropertyType.CURRENTSTATES, 4,
        "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}/{resourceName}");
    addEntry(PropertyType.CURRENTSTATES, 5,
        "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}/{resourceName}/{bucketName}");
    addEntry(PropertyType.TASKCURRENTSTATES, 2,
        "/{clusterName}/INSTANCES/{instanceName}/TASKCURRENTSTATES");
    addEntry(PropertyType.TASKCURRENTSTATES, 3,
        "/{clusterName}/INSTANCES/{instanceName}/TASKCURRENTSTATES/{sessionId}");
    addEntry(PropertyType.TASKCURRENTSTATES, 4,
        "/{clusterName}/INSTANCES/{instanceName}/TASKCURRENTSTATES/{sessionId}/{resourceName}");
    addEntry(PropertyType.TASKCURRENTSTATES, 5,
        "/{clusterName}/INSTANCES/{instanceName}/TASKCURRENTSTATES/{sessionId}/{resourceName}/{bucketName}");
    addEntry(PropertyType.CUSTOMIZEDSTATES, 2,
        "/{clusterName}/INSTANCES/{instanceName}/CUSTOMIZEDSTATES");
    addEntry(PropertyType.CUSTOMIZEDSTATES, 3,
        "/{clusterName}/INSTANCES/{instanceName}/CUSTOMIZEDSTATES/{customizedStateName}");
    addEntry(PropertyType.CUSTOMIZEDSTATES, 4,
        "/{clusterName}/INSTANCES/{instanceName}/CUSTOMIZEDSTATES/{customizedStateName}/{resourceName}");
    addEntry(PropertyType.STATUSUPDATES, 2,
        "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES");
    addEntry(PropertyType.STATUSUPDATES, 3,
        "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}");
    addEntry(PropertyType.STATUSUPDATES, 4,
        "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}");
    addEntry(PropertyType.STATUSUPDATES, 5,
        "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}/{recordName}");
    addEntry(PropertyType.ERRORS, 2, "/{clusterName}/INSTANCES/{instanceName}/ERRORS");
    addEntry(PropertyType.ERRORS, 3, "/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}");
    addEntry(PropertyType.ERRORS, 4,
        "/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}/{subPath}");
    addEntry(PropertyType.ERRORS, 5,
        "/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}/{subPath}/{recordName}");
    addEntry(PropertyType.INSTANCE_HISTORY, 2, "/{clusterName}/INSTANCES/{instanceName}/HISTORY");
    addEntry(PropertyType.HEALTHREPORT, 2, "/{clusterName}/INSTANCES/{instanceName}/HEALTHREPORT");
    addEntry(PropertyType.HEALTHREPORT, 3,
        "/{clusterName}/INSTANCES/{instanceName}/HEALTHREPORT/{reportName}");
    // CONTROLLER
    addEntry(PropertyType.MESSAGES_CONTROLLER, 1, "/{clusterName}/CONTROLLER/MESSAGES");
    addEntry(PropertyType.MESSAGES_CONTROLLER, 2, "/{clusterName}/CONTROLLER/MESSAGES/{msgId}");
    addEntry(PropertyType.ERRORS_CONTROLLER, 1, "/{clusterName}/CONTROLLER/ERRORS");
    addEntry(PropertyType.ERRORS_CONTROLLER, 2, "/{clusterName}/CONTROLLER/ERRORS/{errorId}");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 1, "/{clusterName}/CONTROLLER/STATUSUPDATES");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 2,
        "/{clusterName}/CONTROLLER/STATUSUPDATES/{subPath}");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 3,
        "/{clusterName}/CONTROLLER/STATUSUPDATES/{subPath}/{recordName}");
    addEntry(PropertyType.LEADER, 1, "/{clusterName}/CONTROLLER/LEADER");
    addEntry(PropertyType.HISTORY, 1, "/{clusterName}/CONTROLLER/HISTORY");
    addEntry(PropertyType.PAUSE, 1, "/{clusterName}/CONTROLLER/PAUSE");
    addEntry(PropertyType.MAINTENANCE, 1, "/{clusterName}/CONTROLLER/MAINTENANCE");
    // @formatter:on

    // RESOURCE
    addEntry(PropertyType.WORKFLOWCONTEXT, 2,
        "/{clusterName}/PROPERTYSTORE/TaskRebalancer/{workflowName}/Context"); // Old

    // TODO: These are the current task framework related paths. In the future, if we decide to use
    // a different structure such as a non-flatten ZNode structure, these paths need to be changed
    // accordingly.
    addEntry(PropertyType.TASK_CONFIG_ROOT, 1, "/{clusterName}/CONFIGS/RESOURCE");
    addEntry(PropertyType.WORKFLOW_CONFIG, 2, "/{clusterName}/CONFIGS/RESOURCE/{workflowName}");
    addEntry(PropertyType.JOB_CONFIG, 3,
        "/{clusterName}/CONFIGS/RESOURCE/{workflowName}" + "_" + "{jobName}");
    addEntry(PropertyType.TASK_CONTEXT_ROOT, 1,
        "/{clusterName}/PROPERTYSTORE" + TaskConstants.REBALANCER_CONTEXT_ROOT);
    addEntry(PropertyType.WORKFLOW_CONTEXT, 2, "/{clusterName}/PROPERTYSTORE"
        + TaskConstants.REBALANCER_CONTEXT_ROOT + "/{workflowName}/Context");
    addEntry(PropertyType.JOB_CONTEXT, 3, "/{clusterName}/PROPERTYSTORE"
        + TaskConstants.REBALANCER_CONTEXT_ROOT + "/{workflowName}" + "_" + "{jobName}/Context");
  }
  static Pattern pattern = Pattern.compile("(\\{.+?\\})");

  private static void addEntry(PropertyType type, int numKeys, String template) {
    if (!templateMap.containsKey(type)) {
      templateMap.put(type, new HashMap<Integer, String>());
    }
    logger.trace("Adding template for type:" + type.getType() + " arguments:" + numKeys
        + " template:" + template);
    templateMap.get(type).put(numKeys, template);
  }

  /**
   * Get the Zookeeper path given the property type, cluster, and parameters
   * @param type
   * @param clusterName
   * @param keys
   * @return a valid path, or null if none exists
   */
  public static String getPath(PropertyType type, String clusterName, String... keys) {
    if (clusterName == null) {
      logger.warn("ClusterName can't be null for type:" + type);
      return null;
    }
    if (keys == null) {
      keys = new String[] {};
    }
    String template = null;
    if (templateMap.containsKey(type)) {
      // keys.length+1 since we add clusterName
      template = templateMap.get(type).get(keys.length + 1);
    }

    String result = null;

    if (template != null) {
      result = template;
      Matcher matcher = pattern.matcher(template);
      int count = 0;
      while (matcher.find()) {
        count = count + 1;
        String var = matcher.group();
        if (count == 1) {
          result = result.replace(var, clusterName);
        } else {
          result = result.replace(var, keys[count - 2]);
        }
      }
    }
    if (result == null || result.indexOf('{') > -1 || result.indexOf('}') > -1) {
      logger.warn("Unable to instantiate template:" + template + " using clusterName:" + clusterName
          + " and keys:" + Arrays.toString(keys));
    }
    return result;
  }

  /**
   * Given a path, find the name of an instance at that path
   * @param path
   * @return a valid instance name, or null if none exists
   */
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

  public static String idealState(String clusterName) {
    return "/" + clusterName + "/IDEALSTATES";
  }

  // Path = /<cluster>/IDEALSTATES/<resourceName>
  public static String idealState(String clusterName, String resourceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/IDEALSTATES/");
    builder.append(resourceName);
    return builder.toString();
  }

  public static String stateModelDef(String clusterName) {
    return "/" + clusterName + "/STATEMODELDEFS";
  }

  // Path = /<cluster>/STATEMODELDEFS/<stateModelName>
  public static String stateModelDef(String clusterName, String stateModelName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/STATEMODELDEFS/");
    builder.append(stateModelName);
    return builder.toString();
  }

  public static String externalView(String clusterName) {
    return "/" + clusterName + "/EXTERNALVIEW";
  }

  // Path = /<cluster>/EXTERNALVIEW/<resouceName>
  public static String externalView(String clusterName, String resourceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/EXTERNALVIEW/");
    builder.append(resourceName);
    return builder.toString();
  }

  public static String targetExternalView(String clusterName) {
    return "/" + clusterName + "/TARGETEXTERNALVIEW";
  }

  // Path = /<cluster>/TARGETEXTERNALVIEW/<resouceName>
  public static String targetExternalView(String clusterName, String resourceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/TARGETEXTERNALVIEW/");
    builder.append(resourceName);
    return builder.toString();
  }

  public static String customizedView(String clusterName) {
    return "/" + clusterName + "/CUSTOMIZEDVIEW";
  }

  // Path = /<cluster>/CUSTOMIZEDVIEW/<customizedStateName>
  public static String customizedView(String clusterName, String customizedStateName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/CUSTOMIZEDVIEW/");
    builder.append(customizedStateName);
    return builder.toString();
  }

  // Path = /<cluster>/CUSTOMIZEDVIEW/<customizedStateName>/<resourceName>
  public static String customizedView(String clusterName, String customizedStateName,
      String resourceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/CUSTOMIZEDVIEW/");
    builder.append(customizedStateName);
    builder.append("/");
    builder.append(resourceName);
    return builder.toString();
  }

  public static String liveInstance(String clusterName) {
    return "/" + clusterName + "/LIVEINSTANCES";
  }

  // Path = /<clusterName>/LIVEINSTANCES/<instanceName>
  public static String liveInstance(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/LIVEINSTANCES/");
    builder.append(instanceName);
    return builder.toString();
  }

  public static String instance(String clusterName) {
    return "/" + clusterName + "/INSTANCES";
  }

  @Deprecated
  public static String instanceProperty(String clusterName, String instanceName, PropertyType type,
      String key) {
    return String.format("/%s/INSTANCES/%s/%s/%s", clusterName, instanceName, type, key);
  }

  // Path = /<cluster>/INSTANCES/<instance>
  public static String instance(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/MESSAGES
  public static String instanceMessage(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/MESSAGES");
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/MESSAGES/<messageId>
  public static String instanceMessage(String clusterName, String instanceName, String messageId) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/MESSAGES/");
    builder.append(messageId);
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/CURRENTSTATES
  public static String instanceCurrentState(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/CURRENTSTATES");
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/CURRENTSTATES/<session>
  public static String instanceCurrentState(String clusterName, String instanceName,
      String sessionId) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/CURRENTSTATES/");
    builder.append(sessionId);
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/CURRENTSTATES/<session>/<resourceName>
  public static String instanceCurrentState(String clusterName, String instanceName,
      String sessionId, String resourceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/CURRENTSTATES/");
    builder.append(sessionId);
    builder.append("/");
    builder.append(resourceName);
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/TASKCURRENTSTATES
  public static String instanceTaskCurrentState(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/TASKCURRENTSTATES");
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/TASKCURRENTSTATES/<session>
  public static String instanceTaskCurrentState(String clusterName, String instanceName,
      String sessionId) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/TASKCURRENTSTATES/");
    builder.append(sessionId);
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/TASKCURRENTSTATES/<session>/<resourceName>
  public static String instanceTaskCurrentState(String clusterName, String instanceName,
      String sessionId, String resourceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/TASKCURRENTSTATES/");
    builder.append(sessionId);
    builder.append("/");
    builder.append(resourceName);
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/CUSTOMIZEDSTATES/
  public static String instanceCustomizedState(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/CUSTOMIZEDSTATES");
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/CUSTOMIZEDSTATES/<customizedStateName>
  public static String instanceCustomizedState(String clusterName, String instanceName,
      String customizedStateName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/CUSTOMIZEDSTATES/");
    builder.append(customizedStateName);
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/CUSTOMIZEDSTATES/<customizedStateName>/<resourceName>
  public static String instanceCustomizedState(String clusterName, String instanceName,
      String customizedStateName, String resourceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/CUSTOMIZEDSTATES/");
    builder.append(customizedStateName);
    builder.append("/");
    builder.append(resourceName);
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/ERRORS
  public static String instanceError(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/ERRORS");
    return builder.toString();
  }

  // Path = /<cluster>/INSTANCES/<instance>/ERRORS/<session>/<resourceName>/<partitionName>
  public static String instanceError(String clusterName, String instanceName, String sessionId,
      String resourceName, String partitionName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/ERRORS/");
    builder.append(sessionId);
    builder.append("/");
    builder.append(resourceName);
    builder.append("/");
    builder.append(partitionName);
    return builder.toString();
  }

  // Path = /<clusterName>/INSTANCES/<instanceName>/HISTORY
  public static String instanceHistory(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/HISTORY");
    return builder.toString();
  }

  // Path = /<clusterName>/INSTANCES/<instanceName>/STATUSUPDATES
  public static String instanceStatusUpdate(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/INSTANCES/");
    builder.append(instanceName);
    builder.append("/STATUSUPDATES");
    return builder.toString();
  }

  public static String propertyStore(String clusterName) {
    return "/" + clusterName + "/PROPERTYSTORE";
  }

  // PATH = "/<clusterName>/CONFIGS/CLUSTER/<clusterName>"
  public static String clusterConfig(String clusterName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/CONFIGS/CLUSTER/");
    builder.append(clusterName);
    return builder.toString();
  }

  public static String instanceConfig(String clusterName) {
    return "/" + clusterName + "/CONFIGS/PARTICIPANT";
  }

  // PATH = "/<clusterName>/CONFIGS/PARTICIPANT/<instanceName>"
  public static String instanceConfig(String clusterName, String instanceName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/CONFIGS/PARTICIPANT/");
    builder.append(instanceName);
    return builder.toString();
  }

  public static String resourceConfig(String clusterName) {
    return "/" + clusterName + "/CONFIGS/RESOURCE";
  }

  public static String customizedStateConfig(String clusterName) {
    return "/" + clusterName + "/CONFIGS/CUSTOMIZED_STATE";
  }

  public static String controller(String clusterName) {
    return "/" + clusterName + "/CONTROLLER";
  }

  public static String controllerLeader(String clusterName) {
    return "/" + clusterName + "/CONTROLLER/LEADER";
  }

  public static String controllerMessage(String clusterName) {
    return "/" + clusterName + "/CONTROLLER/MESSAGES";
  }

  // PATH = "/<clusterName>/CONTROLLER/MESSAGES/<messageId>"
  public static String controllerMessage(String clusterName, String messageId) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/CONTROLLER/MESSAGES/");
    builder.append(messageId);
    return builder.toString();
  }

  public static String controllerStatusUpdate(String clusterName) {
    return "/" + clusterName + "/CONTROLLER/STATUSUPDATES";
  }

  // Path /<clusterName>/CONTROLLER/STATUSUPDATES/<subPath>/<recordName>"
  public static String controllerStatusUpdate(String clusterName, String subPath,
      String recordName) {
    StringBuilder builder = new StringBuilder("/");
    builder.append(clusterName);
    builder.append("/CONTROLLER/STATUSUPDATES/");
    builder.append(subPath);
    builder.append("/");
    builder.append(recordName);
    return builder.toString();
  }

  public static String controllerError(String clusterName) {
    return "/" + clusterName + "/CONTROLLER/ERRORS";
  }

  public static String controllerHistory(String clusterName) {
    return String.format("/%s/CONTROLLER/HISTORY", clusterName);
  }

  public static String pause(String clusterName) {
    return "/" + clusterName + "/CONTROLLER/PAUSE";
  }

  public static String maintenance(String clusterName) {
    return "/" + clusterName + "/CONTROLLER/MAINTENANCE";
  }

  // PATH = "/<clusterName>/STATUS/CLUSTER/<clusterName>"
  public static String clusterStatus(String clusterName) {
    return "/" + clusterName + "/STATUS/CLUSTER/" + clusterName;
  }
}
