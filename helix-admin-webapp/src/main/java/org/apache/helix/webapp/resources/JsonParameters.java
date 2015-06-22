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

import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.tools.ClusterSetup;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.data.Form;
import org.restlet.representation.Representation;

public class JsonParameters {
  // json parameter key
  public static final String JSON_PARAMETERS = "jsonParameters";

  // json parameter map keys
  public static final String PARTITION = "partition";
  public static final String RESOURCE = "resource";
  public static final String MANAGEMENT_COMMAND = "command";
  public static final String ENABLED = "enabled";
  public static final String GRAND_CLUSTER = "grandCluster";
  public static final String REPLICAS = "replicas";
  public static final String RESOURCE_KEY_PREFIX = "key";
  public static final String INSTANCE_NAME = "instanceName";
  public static final String INSTANCE_NAMES = "instanceNames";
  public static final String OLD_INSTANCE = "oldInstance";
  public static final String NEW_INSTANCE = "newInstance";
  public static final String CONFIGS = "configs";
  public static final String CONSTRAINT_ATTRIBUTES = "constraintAttributes";
  // public static final String CONSTRAINT_ID = "constraintId";
  public static final String CLUSTER_NAME = "clusterName";
  public static final String PARTITIONS = "partitions";
  public static final String RESOURCE_GROUP_NAME = "resourceGroupName";
  public static final String STATE_MODEL_DEF_REF = "stateModelDefRef";
  public static final String IDEAL_STATE_MODE = "mode";
  public static final String MAX_PARTITIONS_PER_NODE = "maxPartitionsPerNode";
  public static final String BUCKET_SIZE = "bucketSize";

  // zk commands
  public static final String ZK_DELETE_CHILDREN = "zkDeleteChildren";

  // extra json parameter map keys
  public static final String NEW_IDEAL_STATE = "newIdealState";
  public static final String NEW_STATE_MODEL_DEF = "newStateModelDef";

  public static final String TAG = "tag";

  // aliases for ClusterSetup commands
  public static Map<String, Set<String>> CLUSTERSETUP_COMMAND_ALIASES;
  static {
    CLUSTERSETUP_COMMAND_ALIASES = new HashMap<String, Set<String>>();
    CLUSTERSETUP_COMMAND_ALIASES.put(ClusterSetup.addResource,
        new HashSet<String>(Arrays.asList(new String[] {
          "addResourceGroup"
        })));
    CLUSTERSETUP_COMMAND_ALIASES.put(ClusterSetup.activateCluster,
        new HashSet<String>(Arrays.asList(new String[] {
          "enableStorageCluster"
        })));
    CLUSTERSETUP_COMMAND_ALIASES.put(ClusterSetup.addInstance,
        new HashSet<String>(Arrays.asList(new String[] {
          "addInstance"
        })));
  }

  // parameter map
  final Map<String, String> _parameterMap;

  // extra parameters map
  final Map<String, ZNRecord> _extraParameterMap = new HashMap<String, ZNRecord>();

  public JsonParameters(Representation entity) throws Exception {
    this(new Form(entity));
  }

  public JsonParameters(Form form) throws Exception {
    // get parameters in String format
    String jsonPayload = form.getFirstValue(JSON_PARAMETERS, true);
    if (jsonPayload == null || jsonPayload.isEmpty()) {
      _parameterMap = Collections.emptyMap();
    } else {
      _parameterMap = ClusterRepresentationUtil.JsonToMap(jsonPayload);
    }

    // get extra parameters in ZNRecord format
    ObjectMapper mapper = new ObjectMapper();
    String newIdealStateString = form.getFirstValue(NEW_IDEAL_STATE, true);

    if (newIdealStateString != null) {
      ZNRecord newIdealState =
          mapper.readValue(new StringReader(newIdealStateString), ZNRecord.class);
      _extraParameterMap.put(NEW_IDEAL_STATE, newIdealState);
    }

    String newStateModelString = form.getFirstValue(NEW_STATE_MODEL_DEF, true);
    if (newStateModelString != null) {
      ZNRecord newStateModel =
          mapper.readValue(new StringReader(newStateModelString), ZNRecord.class);
      _extraParameterMap.put(NEW_STATE_MODEL_DEF, newStateModel);
    }
  }

  public String getParameter(String key) {
    return _parameterMap.get(key);
  }

  public String getCommand() {
    String command = _parameterMap.get(MANAGEMENT_COMMAND);
    return command;
  }

  public ZNRecord getExtraParameter(String key) {
    return _extraParameterMap.get(key);
  }

  public Map<String, String> cloneParameterMap() {
    Map<String, String> parameterMap = new TreeMap<String, String>();
    parameterMap.putAll(_parameterMap);
    return parameterMap;
  }

  public void verifyCommand(String command) {

    // verify command itself
    if (_parameterMap.isEmpty()) {
      throw new HelixException("'" + JSON_PARAMETERS + "' in the POST body is empty");
    }

    if (!_parameterMap.containsKey(MANAGEMENT_COMMAND)) {
      throw new HelixException("Missing management paramater '" + MANAGEMENT_COMMAND + "'");
    }

    if (!_parameterMap.get(MANAGEMENT_COMMAND).equalsIgnoreCase(command)
        && !(CLUSTERSETUP_COMMAND_ALIASES.get(command) != null && CLUSTERSETUP_COMMAND_ALIASES.get(
            command).contains(_parameterMap.get(MANAGEMENT_COMMAND)))) {
      throw new HelixException(MANAGEMENT_COMMAND + " must be '" + command + "'");
    }

    // verify command parameters
    if (command.equalsIgnoreCase(ClusterSetup.enableInstance)) {
      if (!_parameterMap.containsKey(ENABLED)) {
        throw new HelixException("Missing Json parameters: '" + ENABLED + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.enableResource)) {
      if (!_parameterMap.containsKey(ENABLED)) {
        throw new HelixException("Missing Json parameters: '" + ENABLED + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.enablePartition)) {
      if (!_parameterMap.containsKey(ENABLED)) {
        throw new HelixException("Missing Json parameters: '" + ENABLED + "'");
      }

      if (!_parameterMap.containsKey(PARTITION)) {
        throw new HelixException("Missing Json parameters: '" + PARTITION + "'");
      }

      if (!_parameterMap.containsKey(RESOURCE)) {
        throw new HelixException("Missing Json parameters: '" + RESOURCE + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.resetPartition)) {
      if (!_parameterMap.containsKey(PARTITION)) {
        throw new HelixException("Missing Json parameters: '" + PARTITION + "'");
      }

      if (!_parameterMap.containsKey(RESOURCE)) {
        throw new HelixException("Missing Json parameters: '" + RESOURCE + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.resetInstance)) {
      // nothing
    } else if (command.equalsIgnoreCase(ClusterSetup.activateCluster)) {
      if (!_parameterMap.containsKey(GRAND_CLUSTER)) {
        throw new HelixException("Missing Json parameters: '" + GRAND_CLUSTER + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.setConfig)) {
      if (!_parameterMap.containsKey(CONFIGS)) {
        throw new HelixException("Missing Json parameters: '" + CONFIGS + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.removeConfig)) {
      if (!_parameterMap.containsKey(CONFIGS)) {
        throw new HelixException("Missing Json parameters: '" + CONFIGS + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.addInstanceTag)) {
      if (!_parameterMap.containsKey(ClusterSetup.instanceGroupTag)) {
        throw new HelixException("Missing Json parameters: '" + ClusterSetup.instanceGroupTag + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.removeInstanceTag)) {
      if (!_parameterMap.containsKey(ClusterSetup.instanceGroupTag)) {
        throw new HelixException("Missing Json parameters: '" + ClusterSetup.instanceGroupTag + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.addCluster)) {
      if (!_parameterMap.containsKey(CLUSTER_NAME)) {
        throw new HelixException("Missing Json parameters: '" + CLUSTER_NAME + "'");
      }
    } else if (command.equalsIgnoreCase(ClusterSetup.addResource)) {
      if (!_parameterMap.containsKey(RESOURCE_GROUP_NAME)) {
        throw new HelixException("Missing Json paramaters: '" + RESOURCE_GROUP_NAME + "'");
      }

      if (!_parameterMap.containsKey(PARTITIONS)) {
        throw new HelixException("Missing Json paramaters: '" + PARTITIONS + "'");
      }

      if (!_parameterMap.containsKey(STATE_MODEL_DEF_REF)) {
        throw new HelixException("Missing Json paramaters: '" + STATE_MODEL_DEF_REF + "'");
      }

    }
  }

  // temp test
  public static void main(String[] args) throws Exception {
    String jsonPayload =
        "{\"command\":\"resetPartition\",\"resource\": \"DB-1\",\"partition\":\"DB-1_22 DB-1_23\"}";
    Map<String, String> map = ClusterRepresentationUtil.JsonToMap(jsonPayload);
    System.out.println(map);
  }
}
