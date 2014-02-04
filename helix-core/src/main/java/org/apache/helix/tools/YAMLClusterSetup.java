package org.apache.helix.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.YAMLClusterSetup.YAMLClusterConfig.ParticipantConfig;
import org.apache.helix.tools.YAMLClusterSetup.YAMLClusterConfig.ResourceConfig;
import org.apache.helix.tools.YAMLClusterSetup.YAMLClusterConfig.ResourceConfig.ConstraintsConfig;
import org.apache.helix.tools.YAMLClusterSetup.YAMLClusterConfig.ResourceConfig.StateModelConfig;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

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

/**
 * Supports HelixAdmin operations specified by a YAML configuration file defining a cluster,
 * resources, participants, etc.
 * See the user-rebalanced-lock-manager recipe for an annotated example file.
 */
public class YAMLClusterSetup {
  private static final Logger LOG = Logger.getLogger(YAMLClusterSetup.class);

  private final String _zkAddress;

  /**
   * Start the YAML parser for a given zookeeper instance
   * @param zkAddress
   */
  public YAMLClusterSetup(String zkAddress) {
    _zkAddress = zkAddress;
  }

  /**
   * Set up the cluster by parsing a YAML file.
   * @param input InputStream representing the file
   * @return ClusterConfig Java wrapper of the configuration file
   */
  public YAMLClusterConfig setupCluster(InputStream input) {
    // parse the YAML
    Yaml yaml = new Yaml();
    YAMLClusterConfig cfg = yaml.loadAs(input, YAMLClusterConfig.class);

    // create the cluster
    HelixAdmin helixAdmin = new ZKHelixAdmin(_zkAddress);
    if (cfg.clusterName == null) {
      throw new HelixException("Cluster name is required!");
    }
    helixAdmin.addCluster(cfg.clusterName);

    // add each participant
    if (cfg.participants != null) {
      for (ParticipantConfig participant : cfg.participants) {
        helixAdmin.addInstance(cfg.clusterName, getInstanceCfg(participant));
      }
    }

    // add each resource
    if (cfg.resources != null) {
      for (ResourceConfig resource : cfg.resources) {
        if (resource.name == null) {
          throw new HelixException("Resources must be named!");
        }
        if (resource.stateModel == null || resource.stateModel.name == null) {
          throw new HelixException("Resource must specify a named state model!");
        }
        // if states is null, assume using a built-in or already-added state model
        if (resource.stateModel.states != null) {
          StateModelDefinition stateModelDef =
              getStateModelDef(resource.stateModel, resource.constraints);
          helixAdmin.addStateModelDef(cfg.clusterName, resource.stateModel.name, stateModelDef);
        } else {
          StateModelDefinition stateModelDef = null;
          if (resource.stateModel.name.equals("MasterSlave")) {
            stateModelDef =
                new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
          } else if (resource.stateModel.name.equals("OnlineOffline")) {
            stateModelDef =
                new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline());
          } else if (resource.stateModel.name.equals("LeaderStandby")) {
            stateModelDef =
                new StateModelDefinition(StateModelConfigGenerator.generateConfigForLeaderStandby());
          }
          if (stateModelDef != null) {
            try {
              helixAdmin.addStateModelDef(cfg.clusterName, resource.stateModel.name, stateModelDef);
            } catch (HelixException e) {
              LOG.warn("State model definition " + resource.stateModel.name
                  + " could not be added.");
            }
          }
        }
        int partitions = 1;
        int replicas = 1;
        if (resource.partitions != null) {
          if (resource.partitions.containsKey("count")) {
            partitions = resource.partitions.get("count");
          }
          if (resource.partitions.containsKey("replicas")) {
            replicas = resource.partitions.get("replicas");
          }
        }

        if (resource.rebalancer == null || !resource.rebalancer.containsKey("mode")) {
          throw new HelixException("Rebalance mode is required!");
        }
        helixAdmin.addResource(cfg.clusterName, resource.name, partitions,
            resource.stateModel.name, resource.rebalancer.get("mode"));

        // batch message mode
        if (resource.batchMessageMode != null && resource.batchMessageMode) {
          IdealState idealState = helixAdmin.getResourceIdealState(cfg.clusterName, resource.name);
          idealState.setBatchMessageMode(true);
          helixAdmin.setResourceIdealState(cfg.clusterName, resource.name, idealState);
        }

        // user-defined rebalancer
        if (resource.rebalancer.containsKey("class")
            && resource.rebalancer.get("mode").equals(RebalanceMode.USER_DEFINED.toString())) {
          IdealState idealState = helixAdmin.getResourceIdealState(cfg.clusterName, resource.name);
          idealState.setRebalancerClassName(resource.rebalancer.get("class"));
          helixAdmin.setResourceIdealState(cfg.clusterName, resource.name, idealState);
        }
        helixAdmin.rebalance(cfg.clusterName, resource.name, replicas);
      }
    }

    // enable auto join if this option is set
    if (cfg.autoJoinAllowed != null && cfg.autoJoinAllowed) {
      HelixConfigScope scope =
          new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(cfg.clusterName)
              .build();
      Map<String, String> properties = new HashMap<String, String>();
      properties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, cfg.autoJoinAllowed.toString());
      helixAdmin.setConfig(scope, properties);
    }
    return cfg;
  }

  private static InstanceConfig getInstanceCfg(ParticipantConfig participant) {
    if (participant == null || participant.name == null || participant.host == null
        || participant.port == null) {
      throw new HelixException("Participant must have a specified name, host, and port!");
    }
    InstanceConfig instanceCfg = new InstanceConfig(participant.name);
    instanceCfg.setHostName(participant.host);
    instanceCfg.setPort(participant.port.toString());
    return instanceCfg;
  }

  private static StateModelDefinition getStateModelDef(StateModelConfig stateModel,
      ConstraintsConfig constraints) {
    // Use a builder to define the state model
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(stateModel.name);
    if (stateModel.states == null || stateModel.states.size() == 0) {
      throw new HelixException("List of states are required in a state model!");
    }
    Set<String> stateSet = new HashSet<String>(stateModel.states);
    if (stateModel.initialState == null) {
      throw new HelixException("Initial state is required in a state model!");
    } else if (!stateSet.contains(stateModel.initialState)) {
      throw new HelixException("Initial state is not a valid state");
    }
    builder.initialState(stateModel.initialState);

    // Build a helper for state priorities
    Map<String, Integer> statePriorities = new HashMap<String, Integer>();
    if (constraints != null && constraints.state != null && constraints.state.priorityList != null) {
      int statePriority = 0;
      for (String state : constraints.state.priorityList) {
        if (!stateSet.contains(state)) {
          throw new HelixException("State " + state
              + " in the state priority list is not in the state list!");
        }
        statePriorities.put(state, statePriority);
        statePriority++;
      }
    }

    // Add states, set state priorities
    for (String state : stateModel.states) {
      if (statePriorities.containsKey(state)) {
        builder.addState(state, statePriorities.get(state));
      } else {
        builder.addState(state);
      }
    }

    // Set state counts
    for (Map<String, String> counts : constraints.state.counts) {
      String state = counts.get("name");
      if (!stateSet.contains(state)) {
        throw new HelixException("State " + state + " has a count, but not in the state list!");
      }
      builder.dynamicUpperBound(state, counts.get("count"));
    }

    // Build a helper for transition priorities
    Map<String, Integer> transitionPriorities = new HashMap<String, Integer>();
    if (constraints != null && constraints.transition != null
        && constraints.transition.priorityList != null) {
      int transitionPriority = 0;
      for (String transition : constraints.transition.priorityList) {
        transitionPriorities.put(transition, transitionPriority);
        transitionPriority++;
      }
    }

    // Add the transitions
    if (stateModel.transitions == null || stateModel.transitions.size() == 0) {
      throw new HelixException("Transitions are required!");
    }
    for (Map<String, String> transitions : stateModel.transitions) {
      String name = transitions.get("name");
      String from = transitions.get("from");
      String to = transitions.get("to");
      if (name == null || from == null || to == null) {
        throw new HelixException("All transitions must have a name, a from state, and a to state");
      }
      if (transitionPriorities.containsKey(name)) {
        builder.addTransition(from, to, transitionPriorities.get(name));
      } else {
        builder.addTransition(from, to);
      }
    }

    return builder.build();
  }

  /**
   * Java wrapper for the YAML input file
   */
  public static class YAMLClusterConfig {
    public String clusterName;
    public List<ResourceConfig> resources;
    public List<ParticipantConfig> participants;
    public Boolean autoJoinAllowed;

    public static class ResourceConfig {
      public String name;
      public Map<String, String> rebalancer;
      public Map<String, Integer> partitions;
      public StateModelConfig stateModel;
      public ConstraintsConfig constraints;
      public Boolean batchMessageMode;

      public static class StateModelConfig {
        public String name;
        public List<String> states;
        public List<Map<String, String>> transitions;
        public String initialState;
      }

      public static class ConstraintsConfig {
        public StateConstraintsConfig state;
        public TransitionConstraintsConfig transition;

        public static class StateConstraintsConfig {
          public List<Map<String, String>> counts;
          public List<String> priorityList;
        }

        public static class TransitionConstraintsConfig {
          public List<String> priorityList;
        }
      }
    }

    public static class ParticipantConfig {
      public String name;
      public String host;
      public Integer port;
    }
  }

  /**
   * Start a cluster defined by a YAML file
   * @param args zkAddr, yamlFile
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      LOG.error("USAGE: YAMLClusterSetup zkAddr yamlFile");
      return;
    }
    String zkAddress = args[0];
    String yamlFile = args[1];

    InputStream input;
    try {
      input = new FileInputStream(new File(yamlFile));
    } catch (FileNotFoundException e) {
      LOG.error("Could not open " + yamlFile);
      return;
    }
    new YAMLClusterSetup(zkAddress).setupCluster(input);
  }
}
