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
package com.linkedin.helix.tools;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.HelixAdmin;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.util.ZKClientPool;

public class ClusterSetup
{
  private static Logger      logger            = Logger.getLogger(ClusterSetup.class);
  public static final String zkServerAddress   = "zkSvr";

  // List info about the cluster / DB/ Instances
  public static final String listClusters      = "listClusters";
  public static final String listResources     = "listResources";
  public static final String listInstances     = "listInstances";

  // Add, drop, and rebalance
  public static final String addCluster        = "addCluster";
  public static final String addCluster2       = "addCluster2";
  public static final String dropCluster       = "dropCluster";
  public static final String dropResource      = "dropResource";
  public static final String addInstance       = "addNode";
  public static final String addResource       = "addResource";
  public static final String addStateModelDef  = "addStateModelDef";
  public static final String addIdealState     = "addIdealState";
  public static final String disableInstance   = "disableNode";
  public static final String swapInstance      = "swapInstance";
  public static final String dropInstance      = "dropNode";
  public static final String rebalance         = "rebalance";
  public static final String mode              = "mode";
  public static final String bucketSize        = "bucketSize";
  public static final String resourceKeyPrefix = "key";

  // Query info (TBD in V2)
  public static final String listClusterInfo   = "listClusterInfo";
  public static final String listInstanceInfo  = "listInstanceInfo";
  public static final String listResourceInfo  = "listResourceInfo";
  public static final String listPartitionInfo = "listPartitionInfo";
  public static final String listStateModels   = "listStateModels";
  public static final String listStateModel    = "listStateModel";

  // enable / disable Instances
  public static final String enableInstance    = "enableInstance";
  public static final String enablePartition   = "enablePartition";
  public static final String help              = "help";

  // stats /alerts
  public static final String addStat           = "addStat";
  public static final String addAlert          = "addAlert";
  public static final String dropStat          = "dropStat";
  public static final String dropAlert         = "dropAlert";

  // get/set configs
  public static final String getConfig         = "getConfig";
  public static final String setConfig         = "setConfig";

  static Logger              _logger           = Logger.getLogger(ClusterSetup.class);
  String                     _zkServerAddress;
  ZkClient                   _zkClient;
  HelixAdmin                 _admin;

  public ClusterSetup(String zkServerAddress)
  {
    _zkServerAddress = zkServerAddress;
    _zkClient = ZKClientPool.getZkClient(_zkServerAddress);
    _admin = new ZKHelixAdmin(_zkClient);
  }

  public void addCluster(String clusterName, boolean overwritePrevious)
  {
    _admin.addCluster(clusterName, overwritePrevious);

    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    addStateModelDef(clusterName,
                     "MasterSlave",
                     new StateModelDefinition(generator.generateConfigForMasterSlave()));
    addStateModelDef(clusterName,
                     "LeaderStandby",
                     new StateModelDefinition(generator.generateConfigForLeaderStandby()));
    addStateModelDef(clusterName,
                     "StorageSchemata",
                     new StateModelDefinition(generator.generateConfigForStorageSchemata()));
    addStateModelDef(clusterName,
                     "OnlineOffline",
                     new StateModelDefinition(generator.generateConfigForOnlineOffline()));
  }

  public void addCluster(String clusterName, String grandCluster)
  {
    _admin.addClusterToGrandCluster(clusterName, grandCluster);
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

    ZkClient zkClient = ZKClientPool.getZkClient(_zkServerAddress);

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

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
    ZkClient zkClient = ZKClientPool.getZkClient(_zkServerAddress);

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));
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

  public void dropResourceFromCluster(String clusterName, String resourceName)
  {
    _admin.dropResource(clusterName, resourceName);
  }

  public void rebalanceStorageCluster(String clusterName, String resourceName, int replica)
  {
    rebalanceStorageCluster(clusterName, resourceName, replica, resourceName);
  }

  public void rebalanceStorageCluster(String clusterName,
                                      String resourceName,
                                      int replica,
                                      String keyPrefix)
  {
    List<String> InstanceNames = _admin.getInstancesInCluster(clusterName);
    // ensure we get the same idealState with the same set of instances
    Collections.sort(InstanceNames);

    IdealState idealState = _admin.getResourceIdealState(clusterName, resourceName);
    if (idealState == null)
    {
      throw new HelixException("Resource: " + resourceName + " has NOT been added yet");
    }

    idealState.setReplicas(Integer.toString(replica));
    int partitions = idealState.getNumPartitions();
    String stateModelName = idealState.getStateModelDefRef();
    StateModelDefinition stateModDef =
        _admin.getStateModelDef(clusterName, stateModelName);

    if (stateModDef == null)
    {
      throw new HelixException("cannot find state model: " + stateModelName);
    }
    // StateModelDefinition def = new StateModelDefinition(stateModDef);

    List<String> statePriorityList = stateModDef.getStatesPriorityList();

    String masterStateValue = null;
    String slaveStateValue = null;
    replica--;

    for (String state : statePriorityList)
    {
      String count = stateModDef.getNumInstancesPerState(state);
      if (count.equals("1"))
      {
        if (masterStateValue != null)
        {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        masterStateValue = state;
      }
      else if (count.equalsIgnoreCase("R"))
      {
        if (slaveStateValue != null)
        {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        slaveStateValue = state;
      }
      else if (count.equalsIgnoreCase("N"))
      {
        if (!(masterStateValue == null && slaveStateValue == null))
        {
          throw new HelixException("Invalid or unsupported state model definition");
        }
        replica = InstanceNames.size() - 1;
        masterStateValue = slaveStateValue = state;
      }
    }
    if (masterStateValue == null && slaveStateValue == null)
    {
      throw new HelixException("Invalid or unsupported state model definition");
    }

    if (masterStateValue == null)
    {
      masterStateValue = slaveStateValue;
    }
    if (idealState.getIdealStateMode() != IdealStateModeProperty.AUTO_REBALANCE)
    {
      ZNRecord newIdealState =
          IdealStateCalculatorForStorageNode.calculateIdealState(InstanceNames,
                                                                 partitions,
                                                                 replica,
                                                                 keyPrefix,
                                                                 masterStateValue,
                                                                 slaveStateValue);
      idealState.getRecord().setMapFields(newIdealState.getMapFields());
      idealState.getRecord().setListFields(newIdealState.getListFields());
    }
    else
    {
      for (int i = 0; i < partitions; i++)
      {
        String partitionName = keyPrefix + "_" + i;
        idealState.getRecord().setMapField(partitionName, new HashMap<String, String>());
        idealState.getRecord().setListField(partitionName, new ArrayList<String>());
      }
    }
    _admin.setResourceIdealState(clusterName, resourceName, idealState);
  }

  /**
   * setConfig
   * 
   * @param scopeStr
   *          : scope=value, ... where scope=CLUSTER, RESOURCE, PARTICIPANT, PARTITION
   * @param properitesStr
   *          : key=value, ... which represents a Map<String, String>
   */
  public void setConfig(String scopesStr, String propertiesStr)
  {
    ConfigScope scope = new ConfigScopeBuilder().build(scopesStr);

    // parse properties
    String[] properties = propertiesStr.split("[\\s,]");
    Map<String, String> propertiesMap = new TreeMap<String, String>();
    for (String property : properties)
    {
      int idx = property.indexOf('=');
      if (idx == -1)
      {
        logger.error("Invalid property string: " + property);
        continue;
      }

      String key = property.substring(0, idx);
      String value = property.substring(idx + 1);
      propertiesMap.put(key, value);
    }
    logger.debug("propertiesMap: " + propertiesMap);

    _admin.setConfig(scope, propertiesMap);
  }

  public void removeConfig(String scopesStr, String keysStr)
  {
    ConfigScope scope = new ConfigScopeBuilder().build(scopesStr);

    // parse keys
    String[] keys = keysStr.split("[\\s,]");
    Set<String> keysSet = new HashSet<String>(Arrays.asList(keys));

    _admin.removeConfig(scope, keysSet);
  }

  public String getConfig(String scopesStr, String keysStr)
  {
    ConfigScope scope = new ConfigScopeBuilder().build(scopesStr);

    // parse keys
    String[] keys = keysStr.split("[\\s,]");
    Set<String> keysSet = new HashSet<String>(Arrays.asList(keys));

    Map<String, String> propertiesMap = _admin.getConfig(scope, keysSet);
    StringBuffer sb = new StringBuffer();
    for (String key : keys)
    {
      if (propertiesMap.containsKey(key))
      {
        if (sb.length() > 0)
        {
          sb.append("," + key + "=" + propertiesMap.get(key));
        }
        else
        {
          // sb.length()==0 means the first key=value
          sb.append(key + "=" + propertiesMap.get(key));
        }
      }
      else
      {
        logger.error("Config doesn't exist for key: " + key);
      }
    }

    System.out.println(sb.toString());
    return sb.toString();
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

    Option addClusterOption2 =
        OptionBuilder.withLongOpt(addCluster2)
                     .withDescription("Add a new cluster")
                     .create();
    addClusterOption2.setArgs(2);
    addClusterOption2.setRequired(false);
    addClusterOption2.setArgName("clusterName grandCluster");

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

    Option resourceModeOption =
        OptionBuilder.withLongOpt(mode)
                     .withDescription("Specify resource mode, used with addResourceGroup command")
                     .create();
    resourceModeOption.setArgs(1);
    resourceModeOption.setRequired(false);
    resourceModeOption.setArgName("IdealState mode");
    
    Option resourceBucketSizeOption = OptionBuilder.withLongOpt(bucketSize)
        .withDescription("Specify size of a bucket, used with addResourceGroup command").create();
    resourceBucketSizeOption.setArgs(1);
    resourceBucketSizeOption.setRequired(false);
    resourceBucketSizeOption.setArgName("Size of a bucket for a resource");

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
    addIdealStateOption.setArgName("clusterName reourceName <filename>");

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

    Option InstanceInfoOption =
        OptionBuilder.withLongOpt(listInstanceInfo)
                     .withDescription("Query info of a Instance in a cluster")
                     .create();
    InstanceInfoOption.setArgs(2);
    InstanceInfoOption.setRequired(false);
    InstanceInfoOption.setArgName("clusterName InstanceName");

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

    Option partitionInfoOption =
        OptionBuilder.withLongOpt(listPartitionInfo)
                     .withDescription("Query info of a partition")
                     .create();
    partitionInfoOption.setArgs(3);
    partitionInfoOption.setRequired(false);
    partitionInfoOption.setArgName("clusterName resourceName partitionName");

    Option enableInstanceOption =
        OptionBuilder.withLongOpt(enableInstance)
                     .withDescription("Enable / disable a Instance")
                     .create();
    enableInstanceOption.setArgs(3);
    enableInstanceOption.setRequired(false);
    enableInstanceOption.setArgName("clusterName InstanceName true/false");

    Option enablePartitionOption =
        OptionBuilder.withLongOpt(enablePartition)
                     .withDescription("Enable / disable a partition")
                     .create();
    enablePartitionOption.setArgs(5);
    enablePartitionOption.setRequired(false);
    enablePartitionOption.setArgName("clusterName instanceName resourceName partitionName true/false");

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

    // set/get configs option
    Option setConfOption =
        OptionBuilder.withLongOpt(setConfig).withDescription("Set a config").create();
    setConfOption.setArgs(2);
    setConfOption.setRequired(false);
    setConfOption.setArgName("ConfigScope(scope=value,...) Map(key=value,...");

    Option getConfOption =
        OptionBuilder.withLongOpt(getConfig).withDescription("Get a config").create();
    getConfOption.setArgs(2);
    getConfOption.setRequired(false);
    getConfOption.setArgName("ConfigScope(scope=value,...) Set(key,...");

    OptionGroup group = new OptionGroup();
    group.setRequired(true);
    group.addOption(rebalanceOption);
    group.addOption(addResourceOption);
    group.addOption(resourceModeOption);
    group.addOption(resourceBucketSizeOption);
    group.addOption(resourceKeyOption);
    group.addOption(addClusterOption);
    group.addOption(addClusterOption2);
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
    group.addOption(InstanceInfoOption);
    group.addOption(clusterInfoOption);
    group.addOption(resourceInfoOption);
    group.addOption(partitionInfoOption);
    group.addOption(enableInstanceOption);
    group.addOption(enablePartitionOption);
    group.addOption(addStateModelDefOption);
    group.addOption(listStateModelsOption);
    group.addOption(listStateModelOption);
    group.addOption(addStatOption);
    group.addOption(addAlertOption);
    group.addOption(dropStatOption);
    group.addOption(dropAlertOption);
    group.addOption(setConfOption);
    group.addOption(getConfOption);

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOptionGroup(group);
    return options;
  }

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

  private static boolean checkOptionArgsNumber(Option[] options)
  {
    for (Option option : options)
    {
      int argNb = option.getArgs();
      String[] args = option.getValues();
      if (argNb == 0)
      {
        if (args != null && args.length > 0)
        {
          System.err.println(option.getArgName() + " shall have " + argNb
              + " arguments (was " + Arrays.toString(args) + ")");
          return false;
        }
      }
      else
      {
        if (args == null || args.length != argNb)
        {
          System.err.println(option.getArgName() + " shall have " + argNb
              + " arguments (was " + Arrays.toString(args) + ")");
          return false;
        }
      }
    }
    return true;
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
    boolean ret = checkOptionArgsNumber(cmd.getOptions());
    if (ret == false)
    {
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

    if (cmd.hasOption(addCluster2))
    {
      String clusterName = cmd.getOptionValues(addCluster2)[0];
      String grandCluster = cmd.getOptionValues(addCluster2)[1];
      setupTool.addCluster(clusterName, grandCluster);
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
      if(cmd.hasOption(bucketSize))
      {
        bucketSizeVal = Integer.parseInt(cmd.getOptionValues(bucketSize)[0]);
      }

      setupTool.addResourceToCluster(clusterName,
                                     resourceName,
                                     partitions,
                                     stateModelRef,
                                     modeValue,
                                     bucketSizeVal);
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
      }
      setupTool.rebalanceStorageCluster(clusterName, resourceName, replicas);
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
      // print out partition number, db name and replication number
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
      boolean enabled =
          Boolean.parseBoolean(cmd.getOptionValues(enableInstance)[2].toLowerCase());

      setupTool.getClusterManagementTool().enableInstance(clusterName,
                                                          instanceName,
                                                          enabled);
      return 0;
    }
    else if (cmd.hasOption(enablePartition))
    {
      String clusterName = cmd.getOptionValues(enablePartition)[0];
      String instanceName = cmd.getOptionValues(enablePartition)[1];
      String resourceName = cmd.getOptionValues(enablePartition)[2];
      String partitionName = cmd.getOptionValues(enablePartition)[3];

      boolean enabled =
          Boolean.parseBoolean(cmd.getOptionValues(enablePartition)[4].toLowerCase());

      setupTool.getClusterManagementTool().enablePartition(clusterName,
                                                           instanceName,
                                                           resourceName,
                                                           partitionName,
                                                           enabled);
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

      ZNRecord idealStateRecord =
          (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(idealStateFile)));
      if (idealStateRecord.getId() == null
          || !idealStateRecord.getId().equals(resourceName))
      {
        throw new IllegalArgumentException("ideal state must have same id as resource name");
      }
      setupTool.getClusterManagementTool()
               .setResourceIdealState(clusterName,
                                      resourceName,
                                      new IdealState(idealStateRecord));
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
    else if (cmd.hasOption(setConfig))
    {
      String scopeStr = cmd.getOptionValues(setConfig)[0];
      String propertiesStr = cmd.getOptionValues(setConfig)[1];
      setupTool.setConfig(scopeStr, propertiesStr);
    }
    else if (cmd.hasOption(getConfig))
    {
      String scopeStr = cmd.getOptionValues(getConfig)[0];
      String keysStr = cmd.getOptionValues(getConfig)[1];
      setupTool.getConfig(scopeStr, keysStr);
    }
    else if (cmd.hasOption(help))
    {
      printUsage(cliOptions);
      return 0;
    }
    return 0;
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
