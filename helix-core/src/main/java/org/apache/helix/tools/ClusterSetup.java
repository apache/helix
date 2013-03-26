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
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;


public class ClusterSetup
{
  private static Logger logger = Logger.getLogger(ClusterSetup.class);
  public static final String zkServerAddress = "zkSvr";

  // List info about the cluster / resource / Instances
  public static final String listClusters = "listClusters";
  public static final String listResources = "listResources";
  public static final String listInstances = "listInstances";

  // Add, drop, and rebalance
  public static final String addCluster = "addCluster";
  public static final String activateCluster = "activateCluster";
  public static final String dropCluster = "dropCluster";
  public static final String dropResource = "dropResource";
  public static final String addInstance = "addNode";
  public static final String addResource = "addResource";
  public static final String addStateModelDef = "addStateModelDef";
  public static final String addIdealState = "addIdealState";
  public static final String swapInstance = "swapInstance";
  public static final String dropInstance = "dropNode";
  public static final String rebalance = "rebalance";
  public static final String expandCluster = "expandCluster";
  public static final String expandResource = "expandResource";
  public static final String mode = "mode";
  public static final String bucketSize = "bucketSize";
  public static final String resourceKeyPrefix = "key";
  public static final String maxPartitionsPerNode = "maxPartitionsPerNode";
  
  public static final String addResourceProperty = "addResourceProperty";
  public static final String removeResourceProperty = "removeResourceProperty";

  // Query info (TBD in V2)
  public static final String listClusterInfo = "listClusterInfo";
  public static final String listInstanceInfo = "listInstanceInfo";
  public static final String listResourceInfo = "listResourceInfo";
  public static final String listPartitionInfo = "listPartitionInfo";
  public static final String listStateModels = "listStateModels";
  public static final String listStateModel = "listStateModel";

  // enable/disable/reset instances/cluster/resource/partition
  public static final String enableInstance = "enableInstance";
  public static final String enablePartition = "enablePartition";
  public static final String enableCluster = "enableCluster";
  public static final String resetPartition = "resetPartition";
  public static final String resetInstance = "resetInstance";
  public static final String resetResource = "resetResource";

  // help
  public static final String help = "help";

  // stats/alerts
  public static final String addStat = "addStat";
  public static final String addAlert = "addAlert";
  public static final String dropStat = "dropStat";
  public static final String dropAlert = "dropAlert";

  // get/set/remove configs
  public static final String getConfig = "getConfig";
  public static final String setConfig = "setConfig";
  public static final String removeConfig = "removeConfig";
  
  // get/set/remove constraints
  public static final String getConstraints = "getConstraints";
  public static final String setConstraint = "setConstraint";
  public static final String removeConstraint = "removeConstraint";

  static Logger _logger = Logger.getLogger(ClusterSetup.class);
  String _zkServerAddress;
  ZkClient _zkClient;
  HelixAdmin _admin;

  public ClusterSetup(String zkServerAddress)
  {
    _zkServerAddress = zkServerAddress;
    _zkClient = ZKClientPool.getZkClient(_zkServerAddress);
    _admin = new ZKHelixAdmin(_zkClient);
  }

  public ClusterSetup(ZkClient zkClient)
  {
    _zkServerAddress = zkClient.getServers();
    _zkClient = zkClient;
    _admin = new ZKHelixAdmin(_zkClient);
  }

  public void addCluster(String clusterName, boolean overwritePrevious)
  {
    _admin.addCluster(clusterName, overwritePrevious);

    // StateModelConfigGenerator generator = new StateModelConfigGenerator();
    addStateModelDef(clusterName,
                     "MasterSlave",
                     new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave()));
    addStateModelDef(clusterName,
                     "LeaderStandby",
                     new StateModelDefinition(StateModelConfigGenerator.generateConfigForLeaderStandby()));
    addStateModelDef(clusterName,
                     "StorageSchemata",
                     new StateModelDefinition(StateModelConfigGenerator.generateConfigForStorageSchemata()));
    addStateModelDef(clusterName,
                     "OnlineOffline",
                     new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));
    addStateModelDef(clusterName,
                     "ScheduledTask",
                     new StateModelDefinition(StateModelConfigGenerator.generateConfigForScheduledTaskQueue()));
  }

  public void activateCluster(String clusterName, String grandCluster, boolean enable)
  {
    if (enable)
    {
      _admin.addClusterToGrandCluster(clusterName, grandCluster);
    }
    else
    {
      _admin.dropResource(grandCluster, clusterName);
    }
  }

  public void deleteCluster(String clusterName)
  {
    _admin.dropCluster(clusterName);
  }

  public void addInstancesToCluster(String clusterName, String[] InstanceInfoArray)
  {
    for (String InstanceInfo : InstanceInfoArray)
    {
      // the storage Instance info must be hostname:port format.
      if (InstanceInfo.length() > 0)
      {
        addInstanceToCluster(clusterName, InstanceInfo);
      }
    }
  }

  public void addInstanceToCluster(String clusterName, String InstanceAddress)
  {
    // InstanceAddress must be in host:port format
    int lastPos = InstanceAddress.lastIndexOf("_");
    if (lastPos <= 0)
    {
      lastPos = InstanceAddress.lastIndexOf(":");
    }
    if (lastPos <= 0)
    {
      String error = "Invalid storage Instance info format: " + InstanceAddress;
      _logger.warn(error);
      throw new HelixException(error);
    }
    String host = InstanceAddress.substring(0, lastPos);
    String portStr = InstanceAddress.substring(lastPos + 1);
    int port = Integer.parseInt(portStr);
    addInstanceToCluster(clusterName, host, port);
  }

  public void addInstanceToCluster(String clusterName, String host, int port)
  {
    String instanceId = host + "_" + port;
    InstanceConfig config = new InstanceConfig(instanceId);
    config.setHostName(host);
    config.setPort(Integer.toString(port));
    config.setInstanceEnabled(true);
    _admin.addInstance(clusterName, config);
  }

  public void dropInstancesFromCluster(String clusterName, String[] InstanceInfoArray)
  {
    for (String InstanceInfo : InstanceInfoArray)
    {
      // the storage Instance info must be hostname:port format.
      if (InstanceInfo.length() > 0)
      {
        dropInstanceFromCluster(clusterName, InstanceInfo);
      }
    }
  }

  public void dropInstanceFromCluster(String clusterName, String InstanceAddress)
  {
    // InstanceAddress must be in host:port format
    int lastPos = InstanceAddress.lastIndexOf("_");
    if (lastPos <= 0)
    {
      lastPos = InstanceAddress.lastIndexOf(":");
    }
    if (lastPos <= 0)
    {
      String error = "Invalid storage Instance info format: " + InstanceAddress;
      _logger.warn(error);
      throw new HelixException(error);
    }
    String host = InstanceAddress.substring(0, lastPos);
    String portStr = InstanceAddress.substring(lastPos + 1);
    int port = Integer.parseInt(portStr);
    dropInstanceFromCluster(clusterName, host, port);
  }

  public void dropInstanceFromCluster(String clusterName, String host, int port)
  {
    String instanceId = host + "_" + port;

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    // ensure node is stopped
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceId));
    if (liveInstance != null)
    {
      throw new HelixException("Can't drop " + instanceId + ", please stop " + instanceId
          + " before drop it");
    }

    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instanceId));
    if (config == null)
    {
      String error = "Node " + instanceId + " does not exist, cannot drop";
      _logger.warn(error);
      throw new HelixException(error);
    }

    // ensure node is disabled, otherwise fail
    if (config.getInstanceEnabled())
    {
      String error = "Node " + instanceId + " is enabled, cannot drop";
      _logger.warn(error);
      throw new HelixException(error);
    }
    _admin.dropInstance(clusterName, config);
  }

  public void swapInstance(String clusterName,
                           String oldInstanceName,
                           String newInstanceName)
  {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    InstanceConfig oldConfig =
        accessor.getProperty(keyBuilder.instanceConfig(oldInstanceName));
    if (oldConfig == null)
    {
      String error = "Old instance " + oldInstanceName + " does not exist, cannot swap";
      _logger.warn(error);
      throw new HelixException(error);
    }

    InstanceConfig newConfig =
        accessor.getProperty(keyBuilder.instanceConfig(newInstanceName));
    if (newConfig == null)
    {
      String error = "New instance " + newInstanceName + " does not exist, cannot swap";
      _logger.warn(error);
      throw new HelixException(error);
    }

    // ensure old instance is disabled, otherwise fail
    if (oldConfig.getInstanceEnabled())
    {
      String error =
          "Old instance " + oldInstanceName
              + " is enabled, it need to be disabled and turned off";
      _logger.warn(error);
      throw new HelixException(error);
    }
    // ensure old instance is down, otherwise fail
    List<String> liveInstanceNames =
        accessor.getChildNames(accessor.keyBuilder().liveInstances());

    if (liveInstanceNames.contains(oldInstanceName))
    {
      String error =
          "Old instance " + oldInstanceName
              + " is still on, it need to be disabled and turned off";
      _logger.warn(error);
      throw new HelixException(error);
    }

    dropInstanceFromCluster(clusterName, oldInstanceName);

    List<IdealState> existingIdealStates =
        accessor.getChildValues(accessor.keyBuilder().idealStates());
    for (IdealState idealState : existingIdealStates)
    {
      swapInstanceInIdealState(idealState, oldInstanceName, newInstanceName);
      accessor.setProperty(accessor.keyBuilder()
                                   .idealStates(idealState.getResourceName()), idealState);
    }
  }

  void swapInstanceInIdealState(IdealState idealState,
                                String oldInstance,
                                String newInstance)
  {
    for (String partition : idealState.getRecord().getMapFields().keySet())
    {
      Map<String, String> valMap = idealState.getRecord().getMapField(partition);
      if (valMap.containsKey(oldInstance))
      {
        valMap.put(newInstance, valMap.get(oldInstance));
        valMap.remove(oldInstance);
      }
    }

    for (String partition : idealState.getRecord().getListFields().keySet())
    {
      List<String> valList = idealState.getRecord().getListField(partition);
      for (int i = 0; i < valList.size(); i++)
      {
        if (valList.get(i).equals(oldInstance))
        {
          valList.remove(i);
          valList.add(i, newInstance);
        }
      }
    }
  }

  public HelixAdmin getClusterManagementTool()
  {
    return _admin;
  }

  public void addStateModelDef(String clusterName,
                               String stateModelDef,
                               StateModelDefinition record)
  {
    _admin.addStateModelDef(clusterName, stateModelDef, record);
  }

  public void addResourceToCluster(String clusterName,
                                   String resourceName,
                                   int numResources,
                                   String stateModelRef)
  {
    addResourceToCluster(clusterName,
                         resourceName,
                         numResources,
                         stateModelRef,
                         IdealStateModeProperty.AUTO.toString());
  }

  public void addResourceToCluster(String clusterName,
                                   String resourceName,
                                   int numResources,
                                   String stateModelRef,
                                   String idealStateMode)
  {
    _admin.addResource(clusterName,
                       resourceName,
                       numResources,
                       stateModelRef,
                       idealStateMode);
  }

  public void addResourceToCluster(String clusterName,
                                   String resourceName,
                                   int numResources,
                                   String stateModelRef,
                                   String idealStateMode,
                                   int bucketSize)
  {
    _admin.addResource(clusterName,
                       resourceName,
                       numResources,
                       stateModelRef,
                       idealStateMode,
                       bucketSize);
  }
  
  public void addResourceToCluster(String clusterName,
      String resourceName,
      int numResources,
      String stateModelRef,
      String idealStateMode,
      int bucketSize,
      int maxPartitionsPerInstance)
  {
    _admin.addResource(clusterName,
      resourceName,
      numResources,
      stateModelRef,
      idealStateMode,
      bucketSize,
      maxPartitionsPerInstance);
  }

  public void dropResourceFromCluster(String clusterName, String resourceName)
  {
    _admin.dropResource(clusterName, resourceName);
  }

  // TODO: remove this. has moved to ZkHelixAdmin
  public void rebalanceStorageCluster(String clusterName, String resourceName, int replica)
  {
    rebalanceStorageCluster(clusterName, resourceName, replica, resourceName);
  }

  public void rebalanceResource(String clusterName, String resourceName, int replica)
  {
    rebalanceStorageCluster(clusterName, resourceName, replica, resourceName); 
  }
  public void expandResource(String clusterName, String resourceName)
  {
    IdealState idealState = _admin.getResourceIdealState(clusterName, resourceName);
    if (idealState.getIdealStateMode() == IdealStateModeProperty.AUTO_REBALANCE
        || idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED)
    {
      _logger.info("Skipping idealState " + idealState.getResourceName() + " "
          + idealState.getIdealStateMode());
      return;
    }
    boolean anyLiveInstance = false;
    for (List<String> list : idealState.getRecord().getListFields().values())
    {
      if (list.contains(StateModelToken.ANY_LIVEINSTANCE.toString()))
      {
        _logger.info("Skipping idealState " + idealState.getResourceName()
            + " with ANY_LIVEINSTANCE");
        anyLiveInstance = true;
        continue;
      }
    }
    if (anyLiveInstance)
    {
      return;
    }
    try
    {
      int replica = Integer.parseInt(idealState.getReplicas());
    }
    catch (Exception e)
    {
      _logger.error("", e);
      return;
    }
    if (idealState.getRecord().getListFields().size() == 0)
    {
      _logger.warn("Resource " + resourceName + " not balanced, skip");
      return;
    }
    balanceIdealState(clusterName, idealState);
  }

  public void expandCluster(String clusterName)
  {
    List<String> resources = _admin.getResourcesInCluster(clusterName);
    for (String resourceName : resources)
    {
      expandResource(clusterName, resourceName);
    }
  }

  

  public void balanceIdealState(String clusterName, IdealState idealState)
  {
    // The new instances are added into the cluster already. So we need to find out the
    // instances that
    // already have partitions assigned to them.
    List<String> instanceNames = _admin.getInstancesInCluster(clusterName);
    rebalanceResource(clusterName, idealState, instanceNames);

  }

  private void rebalanceResource(String clusterName,
      IdealState idealState, List<String> instanceNames)
  {
     _admin.rebalance(clusterName, idealState, instanceNames);
  }

  public void rebalanceStorageCluster(String clusterName,
                                      String resourceName,
                                      int replica,
                                      String keyPrefix)
  {
    _admin.rebalance(clusterName, resourceName, replica, keyPrefix);
  }

  
  /**
   * set config
   * 
   * @param scopesKeyValuePairs : csv-formated scope key-value pair. e.g CLUSTER=MyCluster,RESOURCE=MyDB,... 
   *                      where scope-key could be: CLUSTER, RESOURCE, PARTICIPANT, and PARTITION
   * @param keyValuePairs : csv-formatted key-value pairs. e.g. k1=v1,k2=v2,...
   */
  public void setConfig(String scopesKeyValuePairs, String keyValuePairs)
  {
    ConfigScope scope = new ConfigScopeBuilder().build(scopesKeyValuePairs);

    Map<String, String> keyValueMap = HelixUtil.parseCsvFormatedKeyValuePairs(keyValuePairs);
    _admin.setConfig(scope, keyValueMap);
  }

  /**
   * remove config
   * 
   * @param scopesStr : comma-separated scope key-value pair. e.g CLUSTER=MyCluster,RESOURCE=MyDB,... 
   *                      where scope-key could be: CLUSTER, RESOURCE, PARTICIPANT, and PARTITION
   * @param keysStr : comma-separated keys. e.g. k1,k2...
   */
  public void removeConfig(String scopesStr, String keysStr)
  {
    ConfigScope scope = new ConfigScopeBuilder().build(scopesStr);

    // parse keys
    String[] keys = keysStr.split("[\\s,]");
    Set<String> keysSet = new HashSet<String>(Arrays.asList(keys));

    _admin.removeConfig(scope, keysSet);
  }

  /**
   * get config
   * 
   * @param scopesStr : comma-separated scope key-value pair. e.g CLUSTER=MyCluster,RESOURCE=MyDB,... 
   *                      where scope-key could be: CLUSTER, RESOURCE, PARTICIPANT, and PARTITION
   * @param keysStr :  comma-separated keys. e.g. k1,k2...
   * @return : json-formated key-value pair. e.g. {k1=v1,k2=v2,...}
   */
  public String getConfig(String scopesStr, String keysStr)
  {
    
    ConfigScope scope = new ConfigScopeBuilder().build(scopesStr);

    // parse keys
    String[] keys = keysStr.split("[\\s,]");
    Set<String> keysSet = new HashSet<String>(Arrays.asList(keys));

    Map<String, String> propertiesMap = _admin.getConfig(scope, keysSet);
    ZNRecord record = new ZNRecord(scopesStr);
    record.setMapField(scopesStr, propertiesMap);
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    return new String(serializer.serialize(record));
  }

  /**
   * set constraint
   * 
   * @param clusterName
   * @param constraintType
   * @param constraintId
   * @param constraintAttributesMap : csv-formated constraint key-value pairs
   */
  public void setConstraint(String clusterName, String constraintType, String constraintId, String constraintAttributesMap) {
    if (clusterName == null || constraintType == null || constraintId == null || constraintAttributesMap == null) {
      throw new IllegalArgumentException("fail to set constraint. missing clusterName|constraintType|constraintId|constraintAttributesMap");
    }
    
    ConstraintType type = ConstraintType.valueOf(constraintType);
    ConstraintItemBuilder builder = new ConstraintItemBuilder();
    Map<String, String> constraintAttributes = HelixUtil.parseCsvFormatedKeyValuePairs(constraintAttributesMap);
    ConstraintItem constraintItem = builder.addConstraintAttributes(constraintAttributes).build();
    _admin.setConstraint(clusterName, type, constraintId, constraintItem);
  }
  
  /**
   * remove constraint
   * 
   * @param clusterName
   * @param constraintType
   * @param constraintId
   */
  public void removeConstraint(String clusterName, String constraintType, String constraintId) {
    if (clusterName == null || constraintType == null || constraintId == null) {
      throw new IllegalArgumentException("fail to remove constraint. missing clusterName|constraintType|constraintId");
    }

    ConstraintType type = ConstraintType.valueOf(constraintType);
    _admin.removeConstraint(clusterName, type, constraintId);
  }
  
  /**
   * get constraints associated with given type
   * 
   * @param constraintType : constraint-type. e.g. MESSAGE_CONSTRAINT
   * @return json-formated constraints
   */
  public String getConstraints(String clusterName, String constraintType) {
    if (clusterName == null || constraintType == null) {
      throw new IllegalArgumentException("fail to get constraint. missing clusterName|constraintType");
    }

    ConstraintType type = ConstraintType.valueOf(constraintType);
    ClusterConstraints constraints = _admin.getConstraints(clusterName, type);
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    return new String(serializer.serialize(constraints.getRecord()));
  }
  
  /**
   * Sets up a cluster with 6 Instances[localhost:8900 to localhost:8905], 1
   * resource[EspressoDB] with a replication factor of 3
   * 
   * @param clusterName
   */
  public void setupTestCluster(String clusterName)
  {
    addCluster(clusterName, true);
    String storageInstanceInfoArray[] = new String[6];
    for (int i = 0; i < storageInstanceInfoArray.length; i++)
    {
      storageInstanceInfoArray[i] = "localhost:" + (8900 + i);
    }
    addInstancesToCluster(clusterName, storageInstanceInfoArray);
    addResourceToCluster(clusterName, "TestDB", 10, "MasterSlave");
    rebalanceStorageCluster(clusterName, "TestDB", 3);
  }

  public static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + ClusterSetup.class.getName(), cliOptions);
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions()
  {
    Option helpOption =
        OptionBuilder.withLongOpt(help)
                     .withDescription("Prints command-line options info")
                     .create();

    Option zkServerOption =
        OptionBuilder.withLongOpt(zkServerAddress)
                     .withDescription("Provide zookeeper address")
                     .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option listClustersOption =
        OptionBuilder.withLongOpt(listClusters)
                     .withDescription("List existing clusters")
                     .create();
    listClustersOption.setArgs(0);
    listClustersOption.setRequired(false);

    Option listResourceOption =
        OptionBuilder.withLongOpt(listResources)
                     .withDescription("List resources hosted in a cluster")
                     .create();
    listResourceOption.setArgs(1);
    listResourceOption.setRequired(false);
    listResourceOption.setArgName("clusterName");

    Option listInstancesOption =
        OptionBuilder.withLongOpt(listInstances)
                     .withDescription("List Instances in a cluster")
                     .create();
    listInstancesOption.setArgs(1);
    listInstancesOption.setRequired(false);
    listInstancesOption.setArgName("clusterName");

    Option addClusterOption =
        OptionBuilder.withLongOpt(addCluster)
                     .withDescription("Add a new cluster")
                     .create();
    addClusterOption.setArgs(1);
    addClusterOption.setRequired(false);
    addClusterOption.setArgName("clusterName");

    Option activateClusterOption =
        OptionBuilder.withLongOpt(activateCluster)
                     .withDescription("Enable/disable a cluster in distributed controller mode")
                     .create();
    activateClusterOption.setArgs(3);
    activateClusterOption.setRequired(false);
    activateClusterOption.setArgName("clusterName grandCluster true/false");

    Option deleteClusterOption =
        OptionBuilder.withLongOpt(dropCluster)
                     .withDescription("Delete a cluster")
                     .create();
    deleteClusterOption.setArgs(1);
    deleteClusterOption.setRequired(false);
    deleteClusterOption.setArgName("clusterName");

    Option addInstanceOption =
        OptionBuilder.withLongOpt(addInstance)
                     .withDescription("Add a new Instance to a cluster")
                     .create();
    addInstanceOption.setArgs(2);
    addInstanceOption.setRequired(false);
    addInstanceOption.setArgName("clusterName InstanceAddress(host:port)");

    Option addResourceOption =
        OptionBuilder.withLongOpt(addResource)
                     .withDescription("Add a resource to a cluster")
                     .create();
    addResourceOption.setArgs(4);
    addResourceOption.setRequired(false);
    addResourceOption.setArgName("clusterName resourceName partitionNum stateModelRef <-mode modeValue>");

    Option expandResourceOption =
        OptionBuilder.withLongOpt(expandResource)
                     .withDescription("Expand resource to additional nodes")
                     .create();
    expandResourceOption.setArgs(2);
    expandResourceOption.setRequired(false);
    expandResourceOption.setArgName("clusterName resourceName");

    Option expandClusterOption =
        OptionBuilder.withLongOpt(expandCluster)
                     .withDescription("Expand a cluster and all the resources")
                     .create();
    expandClusterOption.setArgs(1);
    expandClusterOption.setRequired(false);
    expandClusterOption.setArgName("clusterName");

    Option resourceModeOption =
        OptionBuilder.withLongOpt(mode)
                     .withDescription("Specify resource mode, used with addResourceGroup command")
                     .create();
    resourceModeOption.setArgs(1);
    resourceModeOption.setRequired(false);
    resourceModeOption.setArgName("IdealState mode");

    Option resourceBucketSizeOption =
        OptionBuilder.withLongOpt(bucketSize)
                     .withDescription("Specify size of a bucket, used with addResourceGroup command")
                     .create();
    resourceBucketSizeOption.setArgs(1);
    resourceBucketSizeOption.setRequired(false);
    resourceBucketSizeOption.setArgName("Size of a bucket for a resource");
    
    Option maxPartitionsPerNodeOption =
        OptionBuilder.withLongOpt(maxPartitionsPerNode)
                     .withDescription("Specify max partitions per node, used with addResourceGroup command")
                     .create();
    maxPartitionsPerNodeOption.setArgs(1);
    maxPartitionsPerNodeOption.setRequired(false);
    maxPartitionsPerNodeOption.setArgName("Max partitions per node for a resource");

    Option resourceKeyOption =
        OptionBuilder.withLongOpt(resourceKeyPrefix)
                     .withDescription("Specify resource key prefix, used with rebalance command")
                     .create();
    resourceKeyOption.setArgs(1);
    resourceKeyOption.setRequired(false);
    resourceKeyOption.setArgName("Resource key prefix");

    Option addStateModelDefOption =
        OptionBuilder.withLongOpt(addStateModelDef)
                     .withDescription("Add a State model to a cluster")
                     .create();
    addStateModelDefOption.setArgs(2);
    addStateModelDefOption.setRequired(false);
    addStateModelDefOption.setArgName("clusterName <filename>");

    Option addIdealStateOption =
        OptionBuilder.withLongOpt(addIdealState)
                     .withDescription("Add a State model to a cluster")
                     .create();
    addIdealStateOption.setArgs(3);
    addIdealStateOption.setRequired(false);
    addIdealStateOption.setArgName("clusterName resourceName <filename>");

    Option dropInstanceOption =
        OptionBuilder.withLongOpt(dropInstance)
                     .withDescription("Drop an existing Instance from a cluster")
                     .create();
    dropInstanceOption.setArgs(2);
    dropInstanceOption.setRequired(false);
    dropInstanceOption.setArgName("clusterName InstanceAddress(host:port)");

    Option swapInstanceOption =
        OptionBuilder.withLongOpt(swapInstance)
                     .withDescription("Swap an old instance from a cluster with a new instance")
                     .create();
    swapInstanceOption.setArgs(3);
    swapInstanceOption.setRequired(false);
    swapInstanceOption.setArgName("clusterName oldInstance newInstance");

    Option dropResourceOption =
        OptionBuilder.withLongOpt(dropResource)
                     .withDescription("Drop an existing resource from a cluster")
                     .create();
    dropResourceOption.setArgs(2);
    dropResourceOption.setRequired(false);
    dropResourceOption.setArgName("clusterName resourceName");

    Option rebalanceOption =
        OptionBuilder.withLongOpt(rebalance)
                     .withDescription("Rebalance a resource in a cluster")
                     .create();
    rebalanceOption.setArgs(3);
    rebalanceOption.setRequired(false);
    rebalanceOption.setArgName("clusterName resourceName replicas");

    Option instanceInfoOption =
        OptionBuilder.withLongOpt(listInstanceInfo)
                     .withDescription("Query info of a Instance in a cluster")
                     .create();
    instanceInfoOption.setArgs(2);
    instanceInfoOption.setRequired(false);
    instanceInfoOption.setArgName("clusterName InstanceName");

    Option clusterInfoOption =
        OptionBuilder.withLongOpt(listClusterInfo)
                     .withDescription("Query info of a cluster")
                     .create();
    clusterInfoOption.setArgs(1);
    clusterInfoOption.setRequired(false);
    clusterInfoOption.setArgName("clusterName");

    Option resourceInfoOption =
        OptionBuilder.withLongOpt(listResourceInfo)
                     .withDescription("Query info of a resource")
                     .create();
    resourceInfoOption.setArgs(2);
    resourceInfoOption.setRequired(false);
    resourceInfoOption.setArgName("clusterName resourceName");

    Option addResourcePropertyOption =
        OptionBuilder.withLongOpt(addResourceProperty)
                     .withDescription("Add a resource property")
                     .create();
    addResourcePropertyOption.setArgs(4);
    addResourcePropertyOption.setRequired(false);
    addResourcePropertyOption.setArgName("clusterName resourceName propertyName propertyValue");

    Option removeResourcePropertyOption =
        OptionBuilder.withLongOpt(removeResourceProperty)
                     .withDescription("Remove a resource property")
                     .create();
    removeResourcePropertyOption.setArgs(3);
    removeResourcePropertyOption.setRequired(false);
    removeResourcePropertyOption.setArgName("clusterName resourceName propertyName");

    Option partitionInfoOption =
        OptionBuilder.withLongOpt(listPartitionInfo)
                     .withDescription("Query info of a partition")
                     .create();
    partitionInfoOption.setArgs(3);
    partitionInfoOption.setRequired(false);
    partitionInfoOption.setArgName("clusterName resourceName partitionName");

    Option enableInstanceOption =
        OptionBuilder.withLongOpt(enableInstance)
                     .withDescription("Enable/disable a Instance")
                     .create();
    enableInstanceOption.setArgs(3);
    enableInstanceOption.setRequired(false);
    enableInstanceOption.setArgName("clusterName InstanceName true/false");

    Option enablePartitionOption =
        OptionBuilder.hasArgs()
                     .withLongOpt(enablePartition)
                     .withDescription("Enable/disable partitions")
                     .create();
    enablePartitionOption.setRequired(false);
    enablePartitionOption.setArgName("true/false clusterName instanceName resourceName partitionName1...");

    Option enableClusterOption =
        OptionBuilder.withLongOpt(enableCluster)
                     .withDescription("pause/resume the controller of a cluster")
                     .create();
    enableClusterOption.setArgs(2);
    enableClusterOption.setRequired(false);
    enableClusterOption.setArgName("clusterName true/false");

    Option resetPartitionOption =
        OptionBuilder.withLongOpt(resetPartition)
                     .withDescription("Reset a partition in error state")
                     .create();
    resetPartitionOption.setArgs(4);
    resetPartitionOption.setRequired(false);
    resetPartitionOption.setArgName("clusterName instanceName resourceName partitionName");

    Option resetInstanceOption =
        OptionBuilder.withLongOpt(resetInstance)
                     .withDescription("Reset all partitions in error state for an instance")
                     .create();
    resetInstanceOption.setArgs(2);
    resetInstanceOption.setRequired(false);
    resetInstanceOption.setArgName("clusterName instanceName");

    Option resetResourceOption =
        OptionBuilder.withLongOpt(resetResource)
                     .withDescription("Reset all partitions in error state for a resource")
                     .create();
    resetResourceOption.setArgs(2);
    resetResourceOption.setRequired(false);
    resetResourceOption.setArgName("clusterName resourceName");

    Option listStateModelsOption =
        OptionBuilder.withLongOpt(listStateModels)
                     .withDescription("Query info of state models in a cluster")
                     .create();
    listStateModelsOption.setArgs(1);
    listStateModelsOption.setRequired(false);
    listStateModelsOption.setArgName("clusterName");

    Option listStateModelOption =
        OptionBuilder.withLongOpt(listStateModel)
                     .withDescription("Query info of a state model in a cluster")
                     .create();
    listStateModelOption.setArgs(2);
    listStateModelOption.setRequired(false);
    listStateModelOption.setArgName("clusterName stateModelName");

    Option addStatOption =
        OptionBuilder.withLongOpt(addStat)
                     .withDescription("Add a persistent stat")
                     .create();
    addStatOption.setArgs(2);
    addStatOption.setRequired(false);
    addStatOption.setArgName("clusterName statName");
    Option addAlertOption =
        OptionBuilder.withLongOpt(addAlert).withDescription("Add an alert").create();
    addAlertOption.setArgs(2);
    addAlertOption.setRequired(false);
    addAlertOption.setArgName("clusterName alertName");

    Option dropStatOption =
        OptionBuilder.withLongOpt(dropStat)
                     .withDescription("Drop a persistent stat")
                     .create();
    dropStatOption.setArgs(2);
    dropStatOption.setRequired(false);
    dropStatOption.setArgName("clusterName statName");
    Option dropAlertOption =
        OptionBuilder.withLongOpt(dropAlert).withDescription("Drop an alert").create();
    dropAlertOption.setArgs(2);
    dropAlertOption.setRequired(false);
    dropAlertOption.setArgName("clusterName alertName");

    // TODO need deal with resource-names containing ","
    // set/get/remove configs options
    Option setConfOption = 
        OptionBuilder.hasArgs(2)
                     .isRequired(false)
                     .withArgName("ConfigScope(e.g. CLUSTER=cluster,RESOURCE=rc,...) KeyValueMap(e.g. k1=v1,k2=v2,...)")
                     .withLongOpt(setConfig)
                     .withDescription("Set a config")
                     .create();

    Option getConfOption = 
        OptionBuilder.hasArgs(2)
                     .isRequired(false)
                     .withArgName("ConfigScope(e.g. CLUSTER=cluster,RESOURCE=rc,...) KeySet(e.g. k1,k2,...)")
                     .withLongOpt(getConfig)
                     .withDescription("Get a config")
                     .create();
    
    Option removeConfOption = 
        OptionBuilder.hasArgs(2)
                     .isRequired(false)
                     .withArgName("ConfigScope(e.g. CLUSTER=cluster,RESOURCE=rc,...) KeySet(e.g. k1,k2,...)")
                     .withLongOpt(getConfig)
                     .withDescription("Remove a config")
                     .create();
    
    // set/get/remove constraints options
    Option setConstraintOption = 
        OptionBuilder.hasArgs(4)
                     .isRequired(false)
                     .withArgName("clusterName ConstraintType(e.g. MESSAGE_CONSTRAINT) ConstraintId ConstraintAttributesMap(e.g. attribute1=valule1,attribute2=value2,...)")
                     .withLongOpt(setConstraint)
                     .withDescription("Set a constraint associated with a give id. create if not exist")
                     .create();

    Option getConstraintsOption = 
        OptionBuilder.hasArgs(2)
                     .isRequired(false)
                     .withArgName("clusterName ConstraintType(e.g. MESSAGE_CONSTRAINT)")
                     .withLongOpt(getConstraints)
                     .withDescription("Get constraints associated with given type")
                     .create();

    Option removeConstraintOption = 
        OptionBuilder.hasArgs(3)
                     .isRequired(false)
                     .withArgName("clusterName ConstraintType(e.g. MESSAGE_CONSTRAINT) ConstraintId")
                     .withLongOpt(removeConstraint)
                     .withDescription("Remove a constraint associated with given id")
                     .create();

    
    OptionGroup group = new OptionGroup();
    group.setRequired(true);
    group.addOption(rebalanceOption);
    group.addOption(addResourceOption);
    group.addOption(resourceModeOption);
    group.addOption(resourceBucketSizeOption);
    group.addOption(maxPartitionsPerNodeOption);
    group.addOption(expandResourceOption);
    group.addOption(expandClusterOption);
    group.addOption(resourceKeyOption);
    group.addOption(addClusterOption);
    group.addOption(activateClusterOption);
    group.addOption(deleteClusterOption);
    group.addOption(addInstanceOption);
    group.addOption(listInstancesOption);
    group.addOption(listResourceOption);
    group.addOption(listClustersOption);
    group.addOption(addIdealStateOption);
    group.addOption(rebalanceOption);
    group.addOption(dropInstanceOption);
    group.addOption(swapInstanceOption);
    group.addOption(dropResourceOption);
    group.addOption(instanceInfoOption);
    group.addOption(clusterInfoOption);
    group.addOption(resourceInfoOption);
    group.addOption(partitionInfoOption);
    group.addOption(enableInstanceOption);
    group.addOption(enablePartitionOption);
    group.addOption(enableClusterOption);
    group.addOption(resetPartitionOption);
    group.addOption(resetInstanceOption);
    group.addOption(resetResourceOption);
    group.addOption(addStateModelDefOption);
    group.addOption(listStateModelsOption);
    group.addOption(listStateModelOption);
    group.addOption(addStatOption);
    group.addOption(addAlertOption);
    group.addOption(dropStatOption);
    group.addOption(dropAlertOption);
    group.addOption(setConfOption);
    group.addOption(getConfOption);
    group.addOption(addResourcePropertyOption);
    group.addOption(removeResourcePropertyOption);
    group.addOption(removeConfOption);
    group.addOption(setConstraintOption);
    group.addOption(getConstraintsOption);
    group.addOption(removeConstraintOption);

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOptionGroup(group);
    return options;
  }
  
  // TODO: remove this. has moved to ZkHelixAdmin
  private static byte[] readFile(String filePath) throws IOException
  {
    File file = new File(filePath);

    int size = (int) file.length();
    byte[] bytes = new byte[size];
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    int read = 0;
    int numRead = 0;
    while (read < bytes.length
        && (numRead = dis.read(bytes, read, bytes.length - read)) >= 0)
    {
      read = read + numRead;
    }
    return bytes;
  }

  public static int processCommandLineArgs(String[] cliArgs) throws Exception
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try
    {
      cmd = cliParser.parse(cliOptions, cliArgs);
    }
    catch (ParseException pe)
    {
      System.err.println("CommandLineClient: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }

    ClusterSetup setupTool = new ClusterSetup(cmd.getOptionValue(zkServerAddress));

    if (cmd.hasOption(addCluster))
    {
      String clusterName = cmd.getOptionValue(addCluster);
      setupTool.addCluster(clusterName, false);
      return 0;
    }

    if (cmd.hasOption(activateCluster))
    {
      String clusterName = cmd.getOptionValues(activateCluster)[0];
      String grandCluster = cmd.getOptionValues(activateCluster)[1];
      boolean enable = Boolean.parseBoolean(cmd.getOptionValues(activateCluster)[2]);
      setupTool.activateCluster(clusterName, grandCluster, enable);
      return 0;
    }

    if (cmd.hasOption(dropCluster))
    {
      String clusterName = cmd.getOptionValue(dropCluster);
      setupTool.deleteCluster(clusterName);
      return 0;
    }

    if (cmd.hasOption(addInstance))
    {
      String clusterName = cmd.getOptionValues(addInstance)[0];
      String InstanceAddressInfo = cmd.getOptionValues(addInstance)[1];
      String[] InstanceAddresses = InstanceAddressInfo.split(";");
      setupTool.addInstancesToCluster(clusterName, InstanceAddresses);
      return 0;
    }

    if (cmd.hasOption(addResource))
    {
      String clusterName = cmd.getOptionValues(addResource)[0];
      String resourceName = cmd.getOptionValues(addResource)[1];
      int partitions = Integer.parseInt(cmd.getOptionValues(addResource)[2]);
      String stateModelRef = cmd.getOptionValues(addResource)[3];
      String modeValue = IdealStateModeProperty.AUTO.toString();
      if (cmd.hasOption(mode))
      {
        modeValue = cmd.getOptionValues(mode)[0];
      }

      int bucketSizeVal = 0;
      if (cmd.hasOption(bucketSize))
      {
        bucketSizeVal = Integer.parseInt(cmd.getOptionValues(bucketSize)[0]);
      }
      
      int maxPartitionsPerNodeVal = -1;
      if (cmd.hasOption(maxPartitionsPerNode))
      {
        maxPartitionsPerNodeVal = Integer.parseInt(cmd.getOptionValues(maxPartitionsPerNode)[0]);
      }
      setupTool.addResourceToCluster(clusterName,
                                     resourceName,
                                     partitions,
                                     stateModelRef,
                                     modeValue,
                                     bucketSizeVal,
                                     maxPartitionsPerNodeVal);
      return 0;
    }

    if (cmd.hasOption(rebalance))
    {
      String clusterName = cmd.getOptionValues(rebalance)[0];
      String resourceName = cmd.getOptionValues(rebalance)[1];
      int replicas = Integer.parseInt(cmd.getOptionValues(rebalance)[2]);
      if (cmd.hasOption(resourceKeyPrefix))
      {
        setupTool.rebalanceStorageCluster(clusterName,
                                          resourceName,
                                          replicas,
                                          cmd.getOptionValue(resourceKeyPrefix));
        return 0;
      }
      setupTool.rebalanceStorageCluster(clusterName, resourceName, replicas);
      return 0;
    }

    if (cmd.hasOption(expandCluster))
    {
      String clusterName = cmd.getOptionValues(expandCluster)[0];

      setupTool.expandCluster(clusterName);
      return 0;
    }

    if (cmd.hasOption(expandResource))
    {
      String clusterName = cmd.getOptionValues(expandResource)[0];
      String resourceName = cmd.getOptionValues(expandResource)[1];
      setupTool.expandResource(clusterName, resourceName);
      return 0;
    }

    if (cmd.hasOption(dropInstance))
    {
      String clusterName = cmd.getOptionValues(dropInstance)[0];
      String InstanceAddressInfo = cmd.getOptionValues(dropInstance)[1];
      String[] InstanceAddresses = InstanceAddressInfo.split(";");
      setupTool.dropInstancesFromCluster(clusterName, InstanceAddresses);
      return 0;
    }

    if (cmd.hasOption(listClusters))
    {
      List<String> clusters = setupTool.getClusterManagementTool().getClusters();

      System.out.println("Existing clusters:");
      for (String cluster : clusters)
      {
        System.out.println(cluster);
      }
      return 0;
    }

    if (cmd.hasOption(listResources))
    {
      String clusterName = cmd.getOptionValue(listResources);
      List<String> resourceNames =
          setupTool.getClusterManagementTool().getResourcesInCluster(clusterName);

      System.out.println("Existing resources in cluster " + clusterName + ":");
      for (String resourceName : resourceNames)
      {
        System.out.println(resourceName);
      }
      return 0;
    }
    else if (cmd.hasOption(listClusterInfo))
    {
      String clusterName = cmd.getOptionValue(listClusterInfo);
      List<String> resourceNames =
          setupTool.getClusterManagementTool().getResourcesInCluster(clusterName);
      List<String> Instances =
          setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);

      System.out.println("Existing resources in cluster " + clusterName + ":");
      for (String resourceName : resourceNames)
      {
        System.out.println(resourceName);
      }

      System.out.println("Instances in cluster " + clusterName + ":");
      for (String InstanceName : Instances)
      {
        System.out.println(InstanceName);
      }
      return 0;
    }
    else if (cmd.hasOption(listInstances))
    {
      String clusterName = cmd.getOptionValue(listInstances);
      List<String> Instances =
          setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);

      System.out.println("Instances in cluster " + clusterName + ":");
      for (String InstanceName : Instances)
      {
        System.out.println(InstanceName);
      }
      return 0;
    }
    else if (cmd.hasOption(listInstanceInfo))
    {
      String clusterName = cmd.getOptionValues(listInstanceInfo)[0];
      String instanceName = cmd.getOptionValues(listInstanceInfo)[1];
      InstanceConfig config =
          setupTool.getClusterManagementTool().getInstanceConfig(clusterName,
                                                                 instanceName);

      String result = new String(new ZNRecordSerializer().serialize(config.getRecord()));
      System.out.println("InstanceConfig: " + result);
      return 0;
    }
    else if (cmd.hasOption(listResourceInfo))
    {
      // print out partition number, resource name and replication number
      // Also the ideal states and current states
      String clusterName = cmd.getOptionValues(listResourceInfo)[0];
      String resourceName = cmd.getOptionValues(listResourceInfo)[1];
      IdealState idealState =
          setupTool.getClusterManagementTool().getResourceIdealState(clusterName,
                                                                     resourceName);
      ExternalView externalView =
          setupTool.getClusterManagementTool().getResourceExternalView(clusterName,
                                                                       resourceName);

      if (idealState != null)
      {
        System.out.println("IdealState for " + resourceName + ":");
        System.out.println(new String(new ZNRecordSerializer().serialize(idealState.getRecord())));
      }
      else
      {
        System.out.println("No idealState for " + resourceName);
      }

      System.out.println();

      if (externalView != null)
      {
        System.out.println("ExternalView for " + resourceName + ":");
        System.out.println(new String(new ZNRecordSerializer().serialize(externalView.getRecord())));
      }
      else
      {
        System.out.println("No externalView for " + resourceName);
      }
      return 0;

    }
    else if (cmd.hasOption(listPartitionInfo))
    {
      // print out where the partition master / slaves locates
      String clusterName = cmd.getOptionValues(listPartitionInfo)[0];
      String resourceName = cmd.getOptionValues(listPartitionInfo)[1];
      String partitionName = cmd.getOptionValues(listPartitionInfo)[2];
      IdealState idealState =
          setupTool.getClusterManagementTool().getResourceIdealState(clusterName,
                                                                     resourceName);
      ExternalView externalView =
          setupTool.getClusterManagementTool().getResourceExternalView(clusterName,
                                                                       resourceName);

      if (idealState != null)
      {
        ZNRecord partInfo = new ZNRecord(resourceName + "/" + partitionName);
        ZNRecord idealStateRec = idealState.getRecord();
        partInfo.setSimpleFields(idealStateRec.getSimpleFields());
        if (idealStateRec.getMapField(partitionName) != null)
        {
          partInfo.setMapField(partitionName, idealStateRec.getMapField(partitionName));
        }
        if (idealStateRec.getListField(partitionName) != null)
        {
          partInfo.setListField(partitionName, idealStateRec.getListField(partitionName));
        }
        System.out.println("IdealState for " + resourceName + "/" + partitionName + ":");
        System.out.println(new String(new ZNRecordSerializer().serialize(partInfo)));
      }
      else
      {
        System.out.println("No idealState for " + resourceName + "/" + partitionName);
      }

      System.out.println();

      if (externalView != null)
      {
        ZNRecord partInfo = new ZNRecord(resourceName + "/" + partitionName);
        ZNRecord extViewRec = externalView.getRecord();
        partInfo.setSimpleFields(extViewRec.getSimpleFields());
        if (extViewRec.getMapField(partitionName) != null)
        {
          partInfo.setMapField(partitionName, extViewRec.getMapField(partitionName));
        }
        if (extViewRec.getListField(partitionName) != null)
        {
          partInfo.setListField(partitionName, extViewRec.getListField(partitionName));
        }

        System.out.println("ExternalView for " + resourceName + "/" + partitionName + ":");
        System.out.println(new String(new ZNRecordSerializer().serialize(partInfo)));
      }
      else
      {
        System.out.println("No externalView for " + resourceName + "/" + partitionName);
      }
      return 0;

    }
    else if (cmd.hasOption(enableInstance))
    {
      String clusterName = cmd.getOptionValues(enableInstance)[0];
      String instanceName = cmd.getOptionValues(enableInstance)[1];
      if (instanceName.contains(":"))
      {
        instanceName = instanceName.replaceAll(":", "_");
      }
      boolean enabled =
          Boolean.parseBoolean(cmd.getOptionValues(enableInstance)[2].toLowerCase());

      setupTool.getClusterManagementTool().enableInstance(clusterName,
                                                          instanceName,
                                                          enabled);
      return 0;
    }
    else if (cmd.hasOption(enablePartition))
    {
      String[] args = cmd.getOptionValues(enablePartition);

      boolean enabled = Boolean.parseBoolean(args[0].toLowerCase());
      String clusterName = args[1];
      String instanceName = args[2];
      String resourceName = args[3];

      List<String> partitionNames =
          Arrays.asList(Arrays.copyOfRange(args, 4, args.length));
      setupTool.getClusterManagementTool().enablePartition(enabled,
                                                           clusterName,
                                                           instanceName,
                                                           resourceName,
                                                           partitionNames);
      return 0;
    }
    else if (cmd.hasOption(resetPartition))
    {
      String[] args = cmd.getOptionValues(resetPartition);

      String clusterName = args[0];
      String instanceName = args[1];
      String resourceName = args[2];
      List<String> partitionNames =
          Arrays.asList(Arrays.copyOfRange(args, 3, args.length));

      setupTool.getClusterManagementTool().resetPartition(clusterName,
                                                          instanceName,
                                                          resourceName,
                                                          partitionNames);
      return 0;
    }
    else if (cmd.hasOption(resetInstance))
    {
      String[] args = cmd.getOptionValues(resetInstance);

      String clusterName = args[0];
      List<String> instanceNames =
          Arrays.asList(Arrays.copyOfRange(args, 1, args.length));

      setupTool.getClusterManagementTool().resetInstance(clusterName, instanceNames);
      return 0;
    }
    else if (cmd.hasOption(resetResource))
    {
      String[] args = cmd.getOptionValues(resetResource);

      String clusterName = args[0];
      List<String> resourceNames =
          Arrays.asList(Arrays.copyOfRange(args, 1, args.length));

      setupTool.getClusterManagementTool().resetResource(clusterName, resourceNames);
      return 0;
    }
    else if (cmd.hasOption(enableCluster))
    {
      String[] params = cmd.getOptionValues(enableCluster);
      String clusterName = params[0];
      boolean enabled = Boolean.parseBoolean(params[1].toLowerCase());
      setupTool.getClusterManagementTool().enableCluster(clusterName, enabled);

      return 0;
    }
    else if (cmd.hasOption(listStateModels))
    {
      String clusterName = cmd.getOptionValues(listStateModels)[0];

      List<String> stateModels =
          setupTool.getClusterManagementTool().getStateModelDefs(clusterName);

      System.out.println("Existing state models:");
      for (String stateModel : stateModels)
      {
        System.out.println(stateModel);
      }
      return 0;
    }
    else if (cmd.hasOption(listStateModel))
    {
      String clusterName = cmd.getOptionValues(listStateModel)[0];
      String stateModel = cmd.getOptionValues(listStateModel)[1];
      StateModelDefinition stateModelDef =
          setupTool.getClusterManagementTool().getStateModelDef(clusterName, stateModel);
      String result =
          new String(new ZNRecordSerializer().serialize(stateModelDef.getRecord()));
      System.out.println("StateModelDefinition: " + result);
      return 0;
    }
    else if (cmd.hasOption(addStateModelDef))
    {
      String clusterName = cmd.getOptionValues(addStateModelDef)[0];
      String stateModelFile = cmd.getOptionValues(addStateModelDef)[1];

      ZNRecord stateModelRecord =
          (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(stateModelFile)));
      if (stateModelRecord.getId() == null || stateModelRecord.getId().length() == 0)
      {
        throw new IllegalArgumentException("ZNRecord for state model definition must have an id");
      }
      setupTool.getClusterManagementTool()
               .addStateModelDef(clusterName,
                                 stateModelRecord.getId(),
                                 new StateModelDefinition(stateModelRecord));
      return 0;
    }
    else if (cmd.hasOption(addIdealState))
    {
      String clusterName = cmd.getOptionValues(addIdealState)[0];
      String resourceName = cmd.getOptionValues(addIdealState)[1];
      String idealStateFile = cmd.getOptionValues(addIdealState)[2];

      setupTool.addIdealState(clusterName, resourceName, idealStateFile);
      return 0;
    }
    else if (cmd.hasOption(addStat))
    {
      String clusterName = cmd.getOptionValues(addStat)[0];
      String statName = cmd.getOptionValues(addStat)[1];

      setupTool.getClusterManagementTool().addStat(clusterName, statName);
    }
    else if (cmd.hasOption(addAlert))
    {
      String clusterName = cmd.getOptionValues(addAlert)[0];
      String alertName = cmd.getOptionValues(addAlert)[1];

      setupTool.getClusterManagementTool().addAlert(clusterName, alertName);
    }
    else if (cmd.hasOption(dropStat))
    {
      String clusterName = cmd.getOptionValues(dropStat)[0];
      String statName = cmd.getOptionValues(dropStat)[1];

      setupTool.getClusterManagementTool().dropStat(clusterName, statName);
    }
    else if (cmd.hasOption(dropAlert))
    {
      String clusterName = cmd.getOptionValues(dropAlert)[0];
      String alertName = cmd.getOptionValues(dropAlert)[1];

      setupTool.getClusterManagementTool().dropAlert(clusterName, alertName);
    }
    else if (cmd.hasOption(dropResource))
    {
      String clusterName = cmd.getOptionValues(dropResource)[0];
      String resourceName = cmd.getOptionValues(dropResource)[1];

      setupTool.getClusterManagementTool().dropResource(clusterName, resourceName);
    }
    else if (cmd.hasOption(swapInstance))
    {
      String clusterName = cmd.getOptionValues(swapInstance)[0];
      String oldInstanceName = cmd.getOptionValues(swapInstance)[1];
      String newInstanceName = cmd.getOptionValues(swapInstance)[2];

      setupTool.swapInstance(clusterName, oldInstanceName, newInstanceName);
    }
    // set/get/remove config options
    else if (cmd.hasOption(setConfig)) {
      String values[] = cmd.getOptionValues(setConfig);
      String scopeStr = values[0];
      String propertiesStr = values[1];
      setupTool.setConfig(scopeStr, propertiesStr);
    } else if (cmd.hasOption(getConfig)) {
      String values[] = cmd.getOptionValues(getConfig);
      String scopeStr = values[0];
      String keySetStr = values[1];
      setupTool.getConfig(scopeStr, keySetStr);
    }  else if (cmd.hasOption(removeConfig)) {
      String values[] = cmd.getOptionValues(removeConfig);
      String scoepStr = values[0];
      String keySetStr = values[1];
      setupTool.removeConfig(scoepStr, keySetStr);
    }
    // set/get/remove constraint options
    else if (cmd.hasOption(setConstraint)) {
      String values[] = cmd.getOptionValues(setConstraint);
      String clusterName = values[0];
      String constraintType = values[1];
      String constraintId = values[2];
      String constraintAttributesMap = values[3];
      setupTool.setConstraint(clusterName, constraintType, constraintId, constraintAttributesMap);
    } else if (cmd.hasOption(getConstraints)) {
      String values[] = cmd.getOptionValues(getConstraints);
      String clusterName = values[0];
      String constraintType = values[1];
      setupTool.getConstraints(clusterName, constraintType);
    } else if (cmd.hasOption(removeConstraint)) {
      String values[] = cmd.getOptionValues(removeConstraint);
      String clusterName = values[0];
      String constraintType = values[1];
      String constraintId = values[2];
      setupTool.removeConstraint(clusterName, constraintType, constraintId);
    }
    // help option
    else if (cmd.hasOption(help))
    {
      printUsage(cliOptions);
      return 0;
    }
    else if (cmd.hasOption(addResourceProperty))
    {
      String clusterName = cmd.getOptionValues(addResourceProperty)[0];
      String resourceName = cmd.getOptionValues(addResourceProperty)[1];
      String propertyKey = cmd.getOptionValues(addResourceProperty)[2];
      String propertyVal = cmd.getOptionValues(addResourceProperty)[3];

      setupTool.addResourceProperty(clusterName, resourceName, propertyKey, propertyVal);
      return 0;
    }
    else if (cmd.hasOption(removeResourceProperty))
    {
      String clusterName = cmd.getOptionValues(removeResourceProperty)[0];
      String resourceName = cmd.getOptionValues(removeResourceProperty)[1];
      String propertyKey = cmd.getOptionValues(removeResourceProperty)[2];

      setupTool.removeResourceProperty(clusterName, resourceName, propertyKey);
      return 0;
    }
    return 0;
  }

  // TODO: remove this. has moved to ZkHelixAdmin
  public void addIdealState(String clusterName, String resourceName, String idealStateFile) throws IOException
  {
    ZNRecord idealStateRecord =
        (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(idealStateFile)));
    if (idealStateRecord.getId() == null
        || !idealStateRecord.getId().equals(resourceName))
    {
      throw new IllegalArgumentException("ideal state must have same id as resource name");
    }
    _admin.setResourceIdealState(clusterName,
                                 resourceName,
                                 new IdealState(idealStateRecord));
  }

  public void addResourceProperty(String clusterName,
                                  String resourceName,
                                  String propertyKey,
                                  String propertyVal)
  {
    IdealState idealState = _admin.getResourceIdealState(clusterName, resourceName);
    if (idealState == null)
    {
      throw new HelixException("Resource: " + resourceName + " has NOT been added yet");
    }
    idealState.getRecord().setSimpleField(propertyKey, propertyVal);
    _admin.setResourceIdealState(clusterName, resourceName, idealState);
  }

  public void removeResourceProperty(String clusterName,
                                     String resourceName,
                                     String propertyKey)
  {
    IdealState idealState = _admin.getResourceIdealState(clusterName, resourceName);
    if (idealState == null)
    {
      throw new HelixException("Resource: " + resourceName + " has NOT been added yet");
    }
    idealState.getRecord().getSimpleFields().remove(propertyKey);
    _admin.setResourceIdealState(clusterName, resourceName, idealState);
  }

  /**
   * @param args
   * @throws Exception
   * @throws JsonMappingException
   * @throws JsonGenerationException
   */
  public static void main(String[] args) throws Exception
  {
    // debug
    // System.out.println("args(" + args.length + "): " + Arrays.asList(args));

    if (args.length == 1 && args[0].equals("setup-test-cluster"))
    {
      System.out.println("By default setting up ");
      new ClusterSetup("localhost:2181").setupTestCluster("storage-integration-cluster");
      new ClusterSetup("localhost:2181").setupTestCluster("relay-integration-cluster");
      System.exit(0);
    }

    int ret = processCommandLineArgs(args);
    System.exit(ret);
  }
}
