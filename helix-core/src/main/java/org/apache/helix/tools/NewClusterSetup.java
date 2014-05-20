package org.apache.helix.tools;

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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Resource;
import org.apache.helix.api.RunningInstance;
import org.apache.helix.api.Scope;
import org.apache.helix.api.Scope.ScopeType;
import org.apache.helix.api.State;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.accessor.ParticipantAccessor;
import org.apache.helix.api.accessor.ResourceAccessor;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ConstraintId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.rebalancer.config.BasicRebalancerConfig;
import org.apache.helix.controller.rebalancer.config.CustomRebalancerConfig;
import org.apache.helix.controller.rebalancer.config.PartitionedRebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfigHolder;
import org.apache.helix.controller.rebalancer.config.SemiAutoRebalancerConfig;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Parse command line and call helix-admin
 */
public class NewClusterSetup {

  private static Logger LOG = Logger.getLogger(NewClusterSetup.class);

  /**
   * List all helix cluster setup options
   */
  public enum HelixOption {
    // help
    help(0, "", "Print command-line options"),

    // zookeeper address
    zkSvr(1, true, "zookeeperServerAddress", "Zookeeper address (host:port, required)"),

    // list cluster/resource/instances
    listClusters(0, "", "List clusters"),
    listResources(1, "clusterId", "List resources in a cluster"),
    listInstances(1, "clusterId", "List instances in a cluster"),

    // add, drop, and rebalance cluster
    addCluster(1, "clusterId", "Add a new cluster"),
    activateCluster(3, "clusterId grandClusterId true/false",
        "Enable/disable a cluster in distributed controller mode"),
    dropCluster(1, "clusterId", "Delete a cluster"),
    dropResource(2, "clusterId resourceId", "Drop a resource from a cluster"),
    addInstance(2, "clusterId instanceId", "Add an instance to a cluster"),
    addResource(4, "clusterId resourceId partitionNumber stateModelDefId",
        "Add a resource to a cluster"),
    addStateModelDef(2, "clusterId jsonFileName", "Add a state model definition to a cluster"),
    addIdealState(2, "clusterId resourceId jsonfileName",
        "Add an ideal state of a resource in cluster"),
    swapInstance(3, "clusterId oldInstanceId newInstanceId",
        "Swap an old instance in cluster with a new instance"),
    dropInstance(2, "clusterId instanceId", "Drop an instance from a cluster"),
    rebalance(3, "clusterId resourceId replicas", "Rebalance a resource in cluster"),
    expandCluster(1, "clusterId", "Expand a cluster"),
    expandResource(2, "clusterId resourceId", "Expand resource to additional nodes"),
    @Deprecated
    mode(1, "rebalancerMode", "Specify rebalancer mode, used with " + addResource + " command"),
    rebalancerMode(1, "rebalancerMode", "Specify rebalancer mode, used with " + addResource
        + " command"),
    instanceGroupTag(1, "instanceGroupTag", "Specify instance group tag, used with " + rebalance
        + " command"),
    bucketSize(1, "bucketSize", "Specify bucket size, used with " + addResource + " command"),
    resourceKeyPrefix(1, "resourceKeyPrefix", "Specify resource key prefix, used with " + rebalance
        + " command"),
    maxPartitionsPerNode(1, "maxPartitionsPerNode", "Specify max partitions per node, used with "
        + addResource + " command"),
    addResourceProperty(4, "clusterId resourceId propertyName propertyValue",
        "Add a resource property"),
    removeResourceProperty(3, "clusterId resourceId propertyName", "Remove a resource property"),
    addInstanceTag(3, "clusterId instanceId tag", "Add a tag to instance"),
    removeInstanceTag(3, "clusterId instanceId tag", "Remove a tag from instance"),

    // query info
    listClusterInfo(1, "clusterId", "Query informaton of a cluster"),
    listInstanceInfo(2, "clusterId instanceId", "Query information of an instance in cluster"),
    listResourceInfo(2, "clusterId resourceId", "Query information of a resource"),
    listPartitionInfo(3, "clusterId resourceId partitionId", "Query information of a partition"),
    listStateModels(1, "clusterId", "Query information of state models in a cluster"),
    listStateModel(2, "clusterId stateModelDefId", "Query information of a state model in cluster"),

    // enable/disable/reset instances/cluster/resource/partition
    enableInstance(3, "clusterId instanceId true/false", "Enable/disable an instance"),
    enablePartition(-1, "true/false clusterId instanceId resourceId partitionId...",
        "Enable/disable partitions"),
    enableCluster(2, "clusterId true/false", "Pause/resume the controller of a cluster"),
    resetPartition(4, "clusterId instanceId resourceId partitionName",
        "Reset a partition in error state"),
    resetInstance(2, "clusterId instanceId", "Reset all partitions in error state for an instance"),
    resetResource(2, "clusterId resourceId", "Reset all partitions in error state for a resource"),

    // stats/alerts
    addStat(2, "clusterId statName", "Add a persistent stat"),
    addAlert(2, "clusterId alertName", "Add an alert"),
    dropStat(2, "clusterId statName", "Drop a persistent stat"),
    dropAlert(2, "clusterId alertName", "Drop an alert"),

    // set/set/remove configs
    getConfig(3, "scope(e.g. RESOURCE) configScopeArgs(e.g. myCluster,testDB) keys(e.g. k1,k2)",
        "Get configs"),
    setConfig(3,
        "scope(e.g. RESOURCE) configScopeArgs(e.g. myCluster,testDB) keyValues(e.g. k1=v1,k2=v2)",
        "Set configs"),
    removeConfig(3, "scope(e.g. RESOURCE) configScopeArgs(e.g. myCluster,testDB) keys(e.g. k1,k2)",
        "Remove configs"),

    // get/set/remove constraints
    getConstraints(2, "clusterId constraintType(e.g. MESSAGE_CONSTRAINT)", "Get constraints"),
    setConstraint(
        4,
        "clusterId constraintType(e.g. MESSAGE_CONSTRAINT) constraintId keyValues(e.g. k1=v1,k2=v2)",
        "Set a constraint, create if not exist"),
    removeConstraint(3, "clusterId constraintType(e.g. MESSAGE_CONSTRAINT) constraintId",
        "Remove a constraint");

    final int _argNum;
    final boolean _isRequired;
    final String _argName;
    final String _description;

    private HelixOption(int argNum, boolean isRequired, String argName, String description) {
      _argNum = argNum;
      _isRequired = isRequired;
      _argName = argName;
      _description = description;
    }

    private HelixOption(int argNum, String argName, String description) {
      this(argNum, false, argName, description);
    }
  }

  private final ZkClient _zkclient;
  private final BaseDataAccessor<ZNRecord> _baseAccessor;

  private NewClusterSetup(ZkClient zkclient) {
    _zkclient = zkclient;
    _baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
  }

  @SuppressWarnings("static-access")
  static Options constructCommandLineOptions() {
    Options options = new Options();

    OptionGroup optionGroup = new OptionGroup();
    for (HelixOption option : HelixOption.values()) {
      Option opt =
          OptionBuilder.withLongOpt(option.name()).hasArgs(option._argNum)
              .isRequired(option._isRequired).withArgName(option._argName)
              .withDescription(option._description).create();
      if (option == HelixOption.help || option == HelixOption.zkSvr) {
        options.addOption(opt);
      } else {
        optionGroup.addOption(opt);
      }
    }
    options.addOptionGroup(optionGroup);
    return options;
  }

  /**
   * Check if we have the right number of arguments
   * @param opt
   * @param optValues
   */
  static void checkArgNum(HelixOption opt, String[] optValues) {

    if (opt._argNum != -1 && opt._argNum < optValues.length) {
      throw new IllegalArgumentException(opt + " should have no less than " + opt._argNum
          + " arguments, but was: " + optValues.length + ", " + Arrays.asList(optValues));
    }
  }

  static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + NewClusterSetup.class.getName(), cliOptions);
  }

  ClusterAccessor clusterAccessor(String clusterName) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    return new ClusterAccessor(ClusterId.from(clusterName), accessor);
  }

  ParticipantAccessor participantAccessor(String clusterName) {
    return new ParticipantAccessor(ClusterId.from(clusterName), new ZKHelixDataAccessor(
        clusterName, _baseAccessor));
  }

  ResourceAccessor resourceAccessor(String clusterName) {
    return new ResourceAccessor(ClusterId.from(clusterName), new ZKHelixDataAccessor(clusterName,
        _baseAccessor));
  }

  void addCluster(String[] optValues) {
    String clusterName = optValues[0];

    List<StateModelDefinition> defaultStateModelDefs = new ArrayList<StateModelDefinition>();
    defaultStateModelDefs.add(new StateModelDefinition(StateModelConfigGenerator
        .generateConfigForMasterSlave()));

    ClusterConfig.Builder builder =
        new ClusterConfig.Builder(ClusterId.from(clusterName))
            .addStateModelDefinitions(defaultStateModelDefs);

    ClusterAccessor accessor = clusterAccessor(clusterName);
    accessor.createCluster(builder.build());
  }

  void addResource(String[] optValues, String[] rebalancerModeValues, String[] bucketSizeValues,
      String[] maxPartitionsPerNodeValues) {
    String clusterName = optValues[0];
    String resourceName = optValues[1];
    int partitionNumber = Integer.parseInt(optValues[2]);
    String stateModelDefName = optValues[3];
    RebalanceMode rebalancerMode =
        rebalancerModeValues == null ? RebalanceMode.SEMI_AUTO : RebalanceMode
            .valueOf(rebalancerModeValues[0]);

    int bucketSize = bucketSizeValues == null ? 0 : Integer.parseInt(bucketSizeValues[0]);

    int maxPartitionsPerNode =
        maxPartitionsPerNodeValues == null ? -1 : Integer.parseInt(maxPartitionsPerNodeValues[0]);

    ResourceId resourceId = ResourceId.from(resourceName);
    StateModelDefId stateModelDefId = StateModelDefId.from(stateModelDefName);

    IdealState idealState = new IdealState(resourceName);
    idealState.setRebalanceMode(rebalancerMode);
    idealState.setNumPartitions(partitionNumber);
    idealState.setMaxPartitionsPerInstance(maxPartitionsPerNode);
    idealState.setStateModelDefId(stateModelDefId);

    RebalancerConfig rebalancerCtx = PartitionedRebalancerConfig.from(idealState);
    ResourceConfig.Builder builder =
        new ResourceConfig.Builder(resourceId).rebalancerConfig(rebalancerCtx).bucketSize(
            bucketSize);

    ClusterAccessor accessor = clusterAccessor(clusterName);
    accessor.addResourceToCluster(builder.build());

  }

  void rebalance(String[] optValues, String[] groupTagValues) {
    String clusterName = optValues[0];
    String resourceName = optValues[1];
    int replicaCount = Integer.parseInt(optValues[2]);
    String groupTag = null;
    if (groupTagValues != null && groupTagValues.length > 0) {
      groupTag = groupTagValues[0];
    }
    ResourceAccessor accessor = resourceAccessor(clusterName);
    accessor.generateDefaultAssignment(ResourceId.from(resourceName), replicaCount, groupTag);
  }

  void addInstance(String[] optValues) {
    String clusterName = optValues[0];
    String[] instanceIds = optValues[1].split(";");

    ClusterAccessor accessor = clusterAccessor(clusterName);
    for (String instanceId : instanceIds) {
      ParticipantConfig.Builder builder =
          new ParticipantConfig.Builder(ParticipantId.from(instanceId));

      accessor.addParticipantToCluster(builder.build());
    }
  }

  void dropCluster(String[] optValues) {
    String clusterName = optValues[0];
    ClusterAccessor accessor = clusterAccessor(clusterName);
    accessor.dropCluster();
  }

  void dropResource(String[] optValues) {
    String clusterName = optValues[0];
    String resourceName = optValues[1];

    ClusterAccessor accessor = clusterAccessor(clusterName);
    accessor.dropResourceFromCluster(ResourceId.from(resourceName));
  }

  void dropInstance(String[] optValues) {
    String clusterName = optValues[0];
    String[] instanceIds = optValues[1].split(";");
    ClusterAccessor accessor = clusterAccessor(clusterName);
    for (String instanceId : instanceIds) {
      accessor.dropParticipantFromCluster(ParticipantId.from(instanceId));
    }

  }

  private static byte[] readFile(String filePath) throws IOException {
    File file = new File(filePath);

    int size = (int) file.length();
    byte[] bytes = new byte[size];
    DataInputStream dis = null;
    try {
      dis = new DataInputStream(new FileInputStream(file));
      int read = 0;
      int numRead = 0;
      while (read < bytes.length && (numRead = dis.read(bytes, read, bytes.length - read)) >= 0) {
        read = read + numRead;
      }
      return bytes;
    } finally {
      if (dis != null) {
        dis.close();
      }
    }
  }

  void addStateModelDef(String[] optValues) {
    String clusterName = optValues[0];
    String stateModelDefJsonFile = optValues[1];

    try {
      StateModelDefinition stateModelDef =
          new StateModelDefinition(
              (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(stateModelDefJsonFile))));
      ClusterAccessor accessor = clusterAccessor(clusterName);
      accessor.addStateModelDefinitionToCluster(stateModelDef);

    } catch (IOException e) {
      LOG.error("Could not parse the state model", e);
    }

  }

  void addIdealState(String[] optValues) {
    String clusterName = optValues[0];
    String resourceName = optValues[1];
    String idealStateJsonFile = optValues[2];

    try {
      IdealState idealState =
          new IdealState(
              (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(idealStateJsonFile))));

      RebalancerConfig rebalancerCtx = PartitionedRebalancerConfig.from(idealState);
      ResourceConfig.Builder builder =
          new ResourceConfig.Builder(ResourceId.from(resourceName)).rebalancerConfig(rebalancerCtx)
              .bucketSize(idealState.getBucketSize());

      ClusterAccessor accessor = clusterAccessor(clusterName);
      accessor.addResourceToCluster(builder.build());
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  void addInstanceTag(String[] optValues) {
    String clusterName = optValues[0];
    String participantName = optValues[1];
    String tag = optValues[2];

    ParticipantAccessor accessor = participantAccessor(clusterName);
    ParticipantId participantId = ParticipantId.from(participantName);

    ParticipantConfig.Delta delta = new ParticipantConfig.Delta(participantId);
    delta.addTag(tag);
    accessor.updateParticipant(participantId, delta);
  }

  void removeInstanceTag(String[] optValues) {
    String clusterName = optValues[0];
    String participantName = optValues[1];
    String tag = optValues[2];

    ParticipantAccessor accessor = participantAccessor(clusterName);
    ParticipantId participantId = ParticipantId.from(participantName);

    ParticipantConfig.Delta delta = new ParticipantConfig.Delta(participantId);
    delta.removeTag(tag);
    accessor.updateParticipant(participantId, delta);
  }

  void listPartitionInfo(String[] optValues) {
    String clusterName = optValues[0];
    String resourceName = optValues[1];
    String partitionName = optValues[2];

    ResourceId resourceId = ResourceId.from(resourceName);
    PartitionId partitionId = PartitionId.from(partitionName);
    ResourceAccessor accessor = resourceAccessor(clusterName);
    Resource resource = accessor.readResource(resourceId);

    StringBuilder sb = new StringBuilder();
    Map<ParticipantId, State> stateMap = resource.getExternalView().getStateMap(partitionId);
    sb.append(resourceName + "/" + partitionName + ", externalView: " + stateMap);
    PartitionedRebalancerConfig partitionedConfig =
        PartitionedRebalancerConfig.from(resource.getRebalancerConfig());
    if (partitionedConfig != null) {
      // for partitioned contexts, check the mode and apply mode-specific information if possible
      if (partitionedConfig.getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
        SemiAutoRebalancerConfig semiAutoConfig =
            BasicRebalancerConfig.convert(resource.getRebalancerConfig(),
                SemiAutoRebalancerConfig.class);
        sb.append(", preferenceList: " + semiAutoConfig.getPreferenceList(partitionId));
      } else if (partitionedConfig.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
        CustomRebalancerConfig customConfig =
            BasicRebalancerConfig.convert(resource.getRebalancerConfig(),
                CustomRebalancerConfig.class);
        sb.append(", preferenceMap: " + customConfig.getPreferenceMap(partitionId));
      }
      if (partitionedConfig.anyLiveParticipant()) {
        sb.append(", anyLiveParticipant: " + partitionedConfig.anyLiveParticipant());
      } else {
        sb.append(", replicaCount: " + partitionedConfig.getReplicaCount());
      }
    }

    System.out.println(sb.toString());
  }

  void enableInstance(String[] optValues) {
    String clusterName = optValues[0];
    String instanceId = optValues[1];
    if (instanceId.indexOf(":") != -1) {
      instanceId = instanceId.replaceAll(":", "_");
    }
    boolean enabled = Boolean.parseBoolean(optValues[2].toLowerCase());

    ParticipantAccessor accessor = participantAccessor(clusterName);
    if (enabled) {
      accessor.enableParticipant(ParticipantId.from(instanceId));
    } else {
      accessor.disableParticipant(ParticipantId.from(instanceId));
    }
  }

  void enablePartition(String[] optValues) {
    boolean enabled = Boolean.parseBoolean(optValues[0].toLowerCase());
    String clusterName = optValues[1];
    ParticipantId participantId = ParticipantId.from(optValues[2]);
    ResourceId resourceId = ResourceId.from(optValues[3]);

    Set<PartitionId> partitionIdSet = new HashSet<PartitionId>();
    for (int i = 4; i < optValues.length; i++) {
      partitionIdSet.add(PartitionId.from(optValues[i]));
    }

    ParticipantAccessor accessor = participantAccessor(clusterName);
    if (enabled) {
      accessor.enablePartitionsForParticipant(participantId, resourceId, partitionIdSet);
    } else {
      accessor.disablePartitionsForParticipant(participantId, resourceId, partitionIdSet);
    }
  }

  void enableCluster(String[] optValues) {
    String clusterName = optValues[0];
    boolean enabled = Boolean.parseBoolean(optValues[1].toLowerCase());

    ClusterAccessor accessor = clusterAccessor(clusterName);
    if (enabled) {
      accessor.resumeCluster();
    } else {
      accessor.pauseCluster();
    }
  }

  /**
   * Convert user config to key value map
   * @param userConfig
   * @param mapKey
   * @param keys
   * @return
   */
  private Map<String, String> keyValueMap(UserConfig userConfig, String mapKey, String[] keys) {
    Map<String, String> results = new HashMap<String, String>();

    for (String key : keys) {
      if (mapKey == null) {
        results.put(key, userConfig.getSimpleField(key));
      } else {
        results.put(key, userConfig.getMapField(mapKey).get(key));
      }
    }
    return results;
  }

  void getConfig(String[] optValues) {
    ScopeType scopeType = ScopeType.valueOf(optValues[0].toUpperCase());
    String[] scopeArgs = optValues[1].split("[\\s,]");
    String[] keys = optValues[2].split("[\\s,]");

    String clusterName = scopeArgs[0];
    Map<String, String> results = null;
    switch (scopeType) {
    case CLUSTER: {
      ClusterAccessor accessor = clusterAccessor(clusterName);
      results = keyValueMap(accessor.readUserConfig(), null, keys);
      break;
    }
    case PARTICIPANT: {
      ParticipantId participantId = ParticipantId.from(scopeArgs[1]);
      ParticipantAccessor accessor = participantAccessor(clusterName);
      results = keyValueMap(accessor.readUserConfig(participantId), null, keys);
      break;
    }
    case RESOURCE: {
      ResourceId resourceId = ResourceId.from(scopeArgs[1]);
      ResourceAccessor accessor = resourceAccessor(clusterName);
      results = keyValueMap(accessor.readUserConfig(resourceId), null, keys);
      break;
    }
    case PARTITION: {
      ResourceId resourceId = ResourceId.from(scopeArgs[1]);
      String partitionId = scopeArgs[2];
      ResourceAccessor accessor = resourceAccessor(clusterName);
      results = keyValueMap(accessor.readUserConfig(resourceId), partitionId, keys);
      break;
    }
    default:
      System.err.println("Non-recognized scopeType: " + scopeType);
      break;
    }

    System.out.println(results);
  }

  /**
   * Convert key-value map to user-config
   * @param scope
   * @param mapKey
   * @param keyValues
   * @return
   */
  private UserConfig userConfig(Scope<?> scope, String mapKey, String[] keyValues) {
    UserConfig userConfig = new UserConfig(scope);

    for (String keyValue : keyValues) {
      String[] splits = keyValue.split("=");
      String key = splits[0];
      String value = splits[1];
      if (mapKey == null) {
        userConfig.setSimpleField(key, value);
      } else {
        if (userConfig.getMapField(mapKey) == null) {
          userConfig.setMapField(mapKey, new TreeMap<String, String>());
        }
        userConfig.getMapField(mapKey).put(key, value);
      }
    }
    return userConfig;
  }

  void setConfig(String[] optValues) {
    ScopeType scopeType = ScopeType.valueOf(optValues[0].toUpperCase());
    String[] scopeArgs = optValues[1].split("[\\s,]");
    String[] keyValues = optValues[2].split("[\\s,]");

    String clusterName = scopeArgs[0];
    Map<String, String> results = new HashMap<String, String>();
    switch (scopeType) {
    case CLUSTER: {
      ClusterAccessor accessor = clusterAccessor(clusterName);
      Scope<ClusterId> scope = Scope.cluster(ClusterId.from(clusterName));
      UserConfig userConfig = userConfig(scope, null, keyValues);
      accessor.setUserConfig(userConfig);
      break;
    }
    case PARTICIPANT: {
      ParticipantId participantId = ParticipantId.from(scopeArgs[1]);
      ParticipantAccessor accessor = participantAccessor(clusterName);
      Scope<ParticipantId> scope = Scope.participant(participantId);
      UserConfig userConfig = userConfig(scope, null, keyValues);
      accessor.setUserConfig(participantId, userConfig);
      break;
    }
    case RESOURCE: {
      ResourceId resourceId = ResourceId.from(scopeArgs[1]);
      ResourceAccessor accessor = resourceAccessor(clusterName);
      Scope<ResourceId> scope = Scope.resource(resourceId);
      UserConfig userConfig = userConfig(scope, null, keyValues);
      accessor.setUserConfig(resourceId, userConfig);
      break;
    }
    case PARTITION: {
      ResourceId resourceId = ResourceId.from(scopeArgs[1]);
      String partitionId = scopeArgs[2];
      ResourceAccessor accessor = resourceAccessor(clusterName);
      Scope<ResourceId> scope = Scope.resource(resourceId);
      UserConfig userConfig = userConfig(scope, partitionId, keyValues);
      accessor.setUserConfig(resourceId, userConfig);
      break;
    }
    default:
      System.err.println("Non-recognized scopeType: " + scopeType);
      break;
    }

    System.out.println(results);
  }

  void setConstraint(String[] optValues) {
    String clusterName = optValues[0];
    String constraintType = optValues[1];
    String constraintId = optValues[2];
    String constraintAttributesMap = optValues[3];
    if (clusterName == null || constraintType == null || constraintId == null
        || constraintAttributesMap == null) {
      System.err
          .println("fail to set constraint. missing clusterName|constraintType|constraintId|constraintAttributesMap");
      return;
    }
    ClusterId clusterId = ClusterId.from(clusterName);
    ClusterAccessor accessor = clusterAccessor(clusterName);
    Map<String, String> constraintAttributes =
        HelixUtil.parseCsvFormatedKeyValuePairs(constraintAttributesMap);
    ConstraintItem item = new ConstraintItem(constraintAttributes);
    ClusterConfig.Delta delta =
        new ClusterConfig.Delta(clusterId).addConstraintItem(
            ConstraintType.valueOf(constraintType), ConstraintId.from(constraintId), item);
    accessor.updateCluster(delta);
  }

  void getConstraints(String[] optValues) {
    String clusterName = optValues[0];
    ConstraintType constraintType = ConstraintType.valueOf(optValues[1]);
    ClusterAccessor accessor = clusterAccessor(clusterName);
    ClusterConstraints constraints = accessor.readConstraints(constraintType);
    System.out.println(constraints.toString());
  }

  void removeConstraint(String[] optValues) {
    String clusterName = optValues[0];
    ConstraintType constraintType = ConstraintType.valueOf(optValues[1]);
    ConstraintId constraintId = ConstraintId.from(optValues[2]);
    ClusterAccessor accessor = clusterAccessor(clusterName);
    accessor.removeConstraint(constraintType, constraintId);
  }

  void listClusterInfo(String[] optValues) {
    String clusterName = optValues[0];
    ClusterAccessor accessor = clusterAccessor(clusterName);
    Set<ResourceId> resources = accessor.readResources().keySet();
    StringBuilder sb =
        new StringBuilder("Existing resources in cluster ").append(clusterName).append(":\n");
    for (ResourceId resourceId : resources) {
      sb.append(resourceId.stringify()).append('\n');
    }
    Set<ParticipantId> participants = accessor.readParticipants().keySet();
    sb.append("Participants in cluster ").append(clusterName).append(":\n");
    for (ParticipantId participantId : participants) {
      sb.append(participantId.stringify()).append('\n');
    }
    System.out.print(sb.toString());
  }

  void listParticipantInfo(String[] optValues) {
    String clusterName = optValues[0];
    String participantName = optValues[1];
    ParticipantAccessor accessor = participantAccessor(clusterName);
    ParticipantId participantId = ParticipantId.from(participantName);
    Participant participant = accessor.readParticipant(participantId);
    StringBuilder sb =
        new StringBuilder("Participant ").append(participantName).append(" in cluster ")
            .append(clusterName).append(":\n").append("hostName: ")
            .append(participant.getHostName()).append(", port: ").append(participant.getPort())
            .append(", enabled: ").append(participant.isEnabled()).append(", disabledPartitions: ")
            .append(participant.getDisabledPartitionIds().toString()).append(", tags:")
            .append(participant.getTags().toString()).append(", currentState: ")
            .append(", messages: ").append(participant.getMessageMap().toString())
            .append(participant.getCurrentStateMap().toString()).append(", alive: ")
            .append(participant.isAlive()).append(", userConfig: ")
            .append(participant.getUserConfig().toString());
    if (participant.isAlive()) {
      RunningInstance runningInstance = participant.getRunningInstance();
      sb.append(", sessionId: ").append(runningInstance.getSessionId().stringify())
          .append(", processId: ").append(runningInstance.getPid().stringify())
          .append(", helixVersion: ").append(runningInstance.getVersion().toString());
    }
    System.out.println(sb.toString());
  }

  void listResourceInfo(String[] optValues) {
    String clusterName = optValues[0];
    String resourceName = optValues[1];
    ResourceAccessor accessor = resourceAccessor(clusterName);
    ResourceId resourceId = ResourceId.from(resourceName);
    Resource resource = accessor.readResource(resourceId);
    RebalancerConfigHolder holder = new RebalancerConfigHolder(resource.getRebalancerConfig());
    StringBuilder sb =
        new StringBuilder("Resource ").append(resourceName).append(" in cluster ")
            .append(clusterName).append(":\n").append("externalView: ")
            .append(resource.getExternalView()).append(", userConfig: ")
            .append(resource.getUserConfig()).append(", rebalancerConfig: ")
            .append(holder.getSerializedConfig());
    System.out.println(sb.toString());
  }

  void listResources(String[] optValues) {
    String clusterName = optValues[0];
    ClusterAccessor accessor = clusterAccessor(clusterName);
    Set<ResourceId> resources = accessor.readResources().keySet();
    StringBuilder sb =
        new StringBuilder("Existing resources in cluster ").append(clusterName).append(":\n");
    for (ResourceId resourceId : resources) {
      sb.append(resourceId.stringify()).append('\n');
    }
    System.out.print(sb.toString());
  }

  void listParticipants(String[] optValues) {
    String clusterName = optValues[0];
    ClusterAccessor accessor = clusterAccessor(clusterName);
    Set<ParticipantId> participants = accessor.readParticipants().keySet();
    StringBuilder sb =
        new StringBuilder("Participants in cluster ").append(clusterName).append(":\n");
    for (ParticipantId participantId : participants) {
      sb.append(participantId.stringify()).append('\n');
    }
    System.out.print(sb.toString());
  }

  void listStateModels(String[] optValues) {
    String clusterName = optValues[0];
    ClusterAccessor accessor = clusterAccessor(clusterName);
    Set<StateModelDefId> stateModelDefs = accessor.readStateModelDefinitions().keySet();
    StringBuilder sb =
        new StringBuilder("State models in cluster ").append(clusterName).append(":\n");
    for (StateModelDefId stateModelDefId : stateModelDefs) {
      sb.append(stateModelDefId.stringify()).append('\n');
    }
    System.out.print(sb.toString());
  }

  void listStateModel(String[] optValues) {
    String clusterName = optValues[0];
    String stateModel = optValues[1];
    StateModelDefId stateModelDefId = StateModelDefId.from(stateModel);
    ClusterAccessor accessor = clusterAccessor(clusterName);
    Map<StateModelDefId, StateModelDefinition> stateModelDefs =
        accessor.readStateModelDefinitions();
    StateModelDefinition stateModelDef = stateModelDefs.get(stateModelDefId);
    StringBuilder sb = new StringBuilder("StateModelDefinition: ").append(stateModelDef.toString());
    System.out.println(sb.toString());
  }

  void listClusters(String[] optValues) {
    List<ClusterId> result = Lists.newArrayList();
    List<String> clusterNames = _baseAccessor.getChildNames("/", 0);
    for (String clusterName : clusterNames) {
      ClusterAccessor accessor = clusterAccessor(clusterName);
      if (accessor.isClusterStructureValid()) {
        result.add(ClusterId.from(clusterName));
      }
    }
    System.out.println("Existing clusters: " + result);
  }

  void removeConfig(String[] optValues) {
    ScopeType type = ScopeType.valueOf(optValues[0].toUpperCase());
    String[] scopeArgs = optValues[1].split("[\\s,]");
    String[] keys = optValues[2].split("[\\s,]");
    String clusterName = scopeArgs[0];
    UserConfig userConfig;
    switch (type) {
    case CLUSTER:
      ClusterAccessor clusterAccessor = clusterAccessor(clusterName);
      userConfig = clusterAccessor.readUserConfig();
      removeKeysFromUserConfig(userConfig, keys);
      clusterAccessor.setUserConfig(userConfig);
      break;
    case RESOURCE:
      ResourceAccessor resourceAccessor = resourceAccessor(clusterName);
      ResourceId resourceId = ResourceId.from(scopeArgs[1]);
      userConfig = resourceAccessor.readUserConfig(resourceId);
      removeKeysFromUserConfig(userConfig, keys);
      resourceAccessor.setUserConfig(resourceId, userConfig);
      break;
    case PARTICIPANT:
      ParticipantAccessor participantAccessor = participantAccessor(clusterName);
      ParticipantId participantId = ParticipantId.from(scopeArgs[1]);
      userConfig = participantAccessor.readUserConfig(participantId);
      removeKeysFromUserConfig(userConfig, keys);
      participantAccessor.setUserConfig(participantId, userConfig);
      break;
    case PARTITION:
      ResourceAccessor resourcePartitionAccessor = resourceAccessor(clusterName);
      PartitionId partitionId = PartitionId.from(scopeArgs[1]);
      userConfig = resourcePartitionAccessor.readUserConfig(partitionId.getResourceId());
      removePartitionFromResourceUserConfig(userConfig, partitionId, keys);
      resourcePartitionAccessor.setUserConfig(partitionId.getResourceId(), userConfig);
      break;
    }
  }

  private void removeKeysFromUserConfig(UserConfig userConfig, String[] keys) {
    Map<String, String> simpleFields = Maps.newHashMap(userConfig.getSimpleFields());
    for (String key : keys) {
      simpleFields.remove(key);
    }
    userConfig.setSimpleFields(simpleFields);
  }

  private void removePartitionFromResourceUserConfig(UserConfig userConfig,
      PartitionId partitionId, String[] keys) {
    Map<String, String> fields = Maps.newHashMap(userConfig.getMapField(partitionId.stringify()));
    for (String key : keys) {
      fields.remove(key);
    }
    userConfig.setMapField(partitionId.stringify(), fields);
  }

  void swapParticipants(String[] optValues) {
    String clusterName = optValues[0];
    String oldParticipantName = optValues[1];
    String newParticipantName = optValues[2];
    ParticipantAccessor accessor = participantAccessor(clusterName);
    accessor.swapParticipants(ParticipantId.from(oldParticipantName),
        ParticipantId.from(newParticipantName));
  }

  void resetPartition(String[] optValues) {
    String clusterName = optValues[0];
    String participantName = optValues[1];
    String resourceName = optValues[2];
    String partitionName = optValues[3];

    Set<PartitionId> partitionIds = ImmutableSet.of(PartitionId.from(partitionName));
    ParticipantAccessor accessor = participantAccessor(clusterName);
    accessor.resetPartitionsForParticipant(ParticipantId.from(participantName),
        ResourceId.from(resourceName), partitionIds);
  }

  void resetResource(String[] optValues) {
    String clusterName = optValues[0];
    String resourceName = optValues[1];
    Set<ResourceId> resourceIds = ImmutableSet.of(ResourceId.from(resourceName));
    ResourceAccessor accessor = resourceAccessor(clusterName);
    accessor.resetResources(resourceIds);
  }

  void resetParticipant(String[] optValues) {
    String clusterName = optValues[0];
    String participantName = optValues[1];
    Set<ParticipantId> participantIds = ImmutableSet.of(ParticipantId.from(participantName));
    ParticipantAccessor accessor = participantAccessor(clusterName);
    accessor.resetParticipants(participantIds);
  }

  void expandResource(String[] optValues) {
    String clusterName = optValues[0];
    String resourceName = optValues[1];
    expandResource(ClusterId.from(clusterName), ResourceId.from(resourceName));
  }

  void expandCluster(String[] optValues) {
    String clusterName = optValues[0];
    ClusterAccessor accessor = clusterAccessor(clusterName);
    Cluster cluster = accessor.readCluster();
    for (ResourceId resourceId : cluster.getResourceMap().keySet()) {
      expandResource(ClusterId.from(clusterName), resourceId);
    }
  }

  private void expandResource(ClusterId clusterId, ResourceId resourceId) {
    ResourceAccessor accessor = resourceAccessor(clusterId.stringify());
    Resource resource = accessor.readResource(resourceId);
    SemiAutoRebalancerConfig config =
        BasicRebalancerConfig.convert(resource.getRebalancerConfig(),
            SemiAutoRebalancerConfig.class);
    if (config == null) {
      LOG.info("Only SEMI_AUTO mode supported for resource expansion");
      return;
    }
    if (config.anyLiveParticipant()) {
      LOG.info("Resource uses ANY_LIVE_PARTICIPANT, skipping default assignment");
      return;
    }
    if (config.getPreferenceLists().size() == 0) {
      LOG.info("No preference lists have been set yet, skipping default assignment");
      return;
    }
    accessor.generateDefaultAssignment(resourceId, -1, null);
  }

  static int processCommandLineArgs(String[] cliArgs) {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try {
      cmd = cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      System.err.println("CommandLineClient: failed to parse command-line options: " + pe);
      printUsage(cliOptions);
      System.exit(1);
    }

    String zkAddr = cmd.getOptionValue(HelixOption.zkSvr.name());
    ZkClient zkclient = null;

    try {
      zkclient =
          new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_SESSION_TIMEOUT,
              new ZNRecordSerializer());

      NewClusterSetup setup = new NewClusterSetup(zkclient);

      Option[] options = cmd.getOptions();

      for (Option option : options) {
        if (option.getLongOpt().equals(HelixOption.zkSvr.name())) {
          continue;
        }

        HelixOption opt = HelixOption.valueOf(option.getLongOpt());
        String[] optValues = cmd.getOptionValues(option.getLongOpt());

        checkArgNum(opt, optValues);

        switch (opt) {
        case listClusters:
          setup.listClusters(optValues);
          break;
        case listResources:
          setup.listResources(optValues);
          break;
        case listInstances:
          setup.listParticipants(optValues);
          break;
        case addCluster:
          setup.addCluster(optValues);
          break;
        case activateCluster:
          break;
        case dropCluster:
          setup.dropCluster(optValues);
          break;
        case dropResource:
          setup.dropResource(optValues);
          break;
        case addInstance:
          setup.addInstance(optValues);
          break;
        case addResource:
          String[] rebalancerModeValues = null;
          if (cmd.hasOption(HelixOption.rebalancerMode.name())) {
            rebalancerModeValues = cmd.getOptionValues(HelixOption.rebalancerMode.name());
            checkArgNum(HelixOption.rebalancerMode, rebalancerModeValues);
          }
          String[] bucketSizeValues = null;
          if (cmd.hasOption(HelixOption.bucketSize.name())) {
            bucketSizeValues = cmd.getOptionValues(HelixOption.bucketSize.name());
            checkArgNum(HelixOption.bucketSize, bucketSizeValues);
          }
          String[] maxPartitionsPerNodeValues = null;
          if (cmd.hasOption(HelixOption.maxPartitionsPerNode.name())) {
            maxPartitionsPerNodeValues =
                cmd.getOptionValues(HelixOption.maxPartitionsPerNode.name());
            checkArgNum(HelixOption.maxPartitionsPerNode, maxPartitionsPerNodeValues);
          }
          setup.addResource(optValues, rebalancerModeValues, bucketSizeValues,
              maxPartitionsPerNodeValues);
          break;
        case addStateModelDef:
          setup.addStateModelDef(optValues);
          break;
        case addIdealState:
          setup.addIdealState(optValues);
          break;
        case swapInstance:
          setup.swapParticipants(optValues);
          break;
        case dropInstance:
          setup.dropInstance(optValues);
          break;
        case rebalance:
          String[] groupTagValues = null;
          if (cmd.hasOption(HelixOption.instanceGroupTag.name())) {
            groupTagValues = cmd.getOptionValues(HelixOption.instanceGroupTag.name());
            checkArgNum(HelixOption.instanceGroupTag, groupTagValues);
          }
          setup.rebalance(optValues, groupTagValues);
          break;
        case expandCluster:
          setup.expandCluster(optValues);
          break;
        case expandResource:
          setup.expandResource(optValues);
          break;
        case mode:
        case rebalancerMode:
        case bucketSize:
        case maxPartitionsPerNode:
          // always used with addResource command
          continue;
        case instanceGroupTag:
          // always used with rebalance command
          continue;
        case resourceKeyPrefix:
          throw new UnsupportedOperationException(HelixOption.resourceKeyPrefix
              + " is not supported, please set partition names directly");
        case addResourceProperty:
          throw new UnsupportedOperationException(HelixOption.addResourceProperty
              + " is not supported, please use setConfig");
        case removeResourceProperty:
          throw new UnsupportedOperationException(HelixOption.removeResourceProperty
              + " is not supported, please use removeConfig");
        case addInstanceTag:
          setup.addInstanceTag(optValues);
          break;
        case removeInstanceTag:
          setup.removeInstanceTag(optValues);
          break;
        case listClusterInfo:
          setup.listClusterInfo(optValues);
          break;
        case listInstanceInfo:
          setup.listParticipantInfo(optValues);
          break;
        case listResourceInfo:
          setup.listResourceInfo(optValues);
          break;
        case listPartitionInfo:
          setup.listPartitionInfo(optValues);
          break;
        case listStateModels:
          setup.listStateModels(optValues);
          break;
        case listStateModel:
          setup.listStateModel(optValues);
          break;
        case enableInstance:
          setup.enableInstance(optValues);
          break;
        case enablePartition:
          setup.enablePartition(optValues);
          break;
        case enableCluster:
          setup.enableCluster(optValues);
          break;
        case resetPartition:
          setup.resetPartition(optValues);
          break;
        case resetInstance:
          setup.resetParticipant(optValues);
          break;
        case resetResource:
          setup.resetResource(optValues);
          break;
        case getConfig:
          setup.getConfig(optValues);
          break;
        case setConfig:
          setup.setConfig(optValues);
          break;
        case removeConfig:
          setup.removeConfig(optValues);
          break;
        case getConstraints:
          setup.getConstraints(optValues);
          break;
        case setConstraint:
          setup.setConstraint(optValues);
          break;
        case removeConstraint:
          setup.removeConstraint(optValues);
          break;
        default:
          System.err.println("Non-recognized option: " + opt);
          break;
        }

        // process 1 option only
        break;
      }

      return 0;
    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }

  public static void main(String[] args) {
    // if (args.length == 1 && args[0].equals("setup-test-cluster")) {
    // System.out
    // .println("By default setting up TestCluster with 6 instances, 10 partitions, Each partition will have 3 replicas");
    // new ClusterSetup("localhost:2181").setupTestCluster("TestCluster");
    // System.exit(0);
    // }

    int ret = processCommandLineArgs(args);
    System.exit(ret);

  }
}
