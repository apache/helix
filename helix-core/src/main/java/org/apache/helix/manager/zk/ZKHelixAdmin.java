package org.apache.helix.manager.zk;

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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigScope;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.ConfigScope.ConfigScopeProperty;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.alerts.AlertsHolder;
import org.apache.helix.alerts.StatsHolder;
import org.apache.helix.model.Alerts;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.PersistentStats;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.tools.IdealStateCalculatorForStorageNode;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;


public class ZKHelixAdmin implements HelixAdmin
{

  private final ZkClient _zkClient;
  private final ConfigAccessor _configAccessor;

  private static Logger logger = Logger.getLogger(ZKHelixAdmin.class);

  public ZKHelixAdmin(ZkClient zkClient)
  {
    _zkClient = zkClient;
    _configAccessor = new ConfigAccessor(zkClient);
  }

  public ZKHelixAdmin(String zkAddress)
  {
    _zkClient = new ZkClient(zkAddress);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    _zkClient.waitUntilConnected(30, TimeUnit.SECONDS);
    _configAccessor = new ConfigAccessor(_zkClient);
  }
  
  @Override
  public void addInstance(String clusterName, InstanceConfig instanceConfig)
  {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }
    String instanceConfigsPath =
        PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                   clusterName,
                                   ConfigScopeProperty.PARTICIPANT.toString());
    String nodeId = instanceConfig.getId();
    String instanceConfigPath = instanceConfigsPath + "/" + nodeId;

    if (_zkClient.exists(instanceConfigPath))
    {
      throw new HelixException("Node " + nodeId + " already exists in cluster "
          + clusterName);
    }

    ZKUtil.createChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord());

    _zkClient.createPersistent(HelixUtil.getMessagePath(clusterName, nodeId), true);
    _zkClient.createPersistent(HelixUtil.getCurrentStateBasePath(clusterName, nodeId),
                               true);
    _zkClient.createPersistent(HelixUtil.getErrorsPath(clusterName, nodeId), true);
    _zkClient.createPersistent(HelixUtil.getStatusUpdatesPath(clusterName, nodeId), true);
  }

  @Override
  public void dropInstance(String clusterName, InstanceConfig instanceConfig)
  {
    // String instanceConfigsPath = HelixUtil.getConfigPath(clusterName);
    String instanceConfigsPath =
        PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                   clusterName,
                                   ConfigScopeProperty.PARTICIPANT.toString());
    String nodeId = instanceConfig.getId();
    String instanceConfigPath = instanceConfigsPath + "/" + nodeId;
    String instancePath = HelixUtil.getInstancePath(clusterName, nodeId);

    if (!_zkClient.exists(instanceConfigPath))
    {
      throw new HelixException("Node " + nodeId
          + " does not exist in config for cluster " + clusterName);
    }

    if (!_zkClient.exists(instancePath))
    {
      throw new HelixException("Node " + nodeId
          + " does not exist in instances for cluster " + clusterName);
    }

    // delete config path
    ZKUtil.dropChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord());

    // delete instance path
    _zkClient.deleteRecursive(instancePath);
  }

  @Override
  public InstanceConfig getInstanceConfig(String clusterName, String instanceName)
  {
    // String instanceConfigsPath = HelixUtil.getConfigPath(clusterName);

    // String instanceConfigPath = instanceConfigsPath + "/" + instanceName;
    String instanceConfigPath =
        PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                   clusterName,
                                   ConfigScopeProperty.PARTICIPANT.toString(),
                                   instanceName);
    if (!_zkClient.exists(instanceConfigPath))
    {
      throw new HelixException("instance" + instanceName + " does not exist in cluster "
          + clusterName);
    }

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.instanceConfig(instanceName));
  }

  @Override
  public void enableInstance(final String clusterName,
                             final String instanceName,
                             final boolean enabled)
  {
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                   clusterName,
                                   ConfigScopeProperty.PARTICIPANT.toString(),
                                   instanceName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient);
    if (!baseAccessor.exists(path, 0))
    {
      throw new HelixException("Cluster " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }

    baseAccessor.update(path, new DataUpdater<ZNRecord>()
    {
      @Override
      public ZNRecord update(ZNRecord currentData)
      {
        if (currentData == null)
        {
          throw new HelixException("Cluster: " + clusterName + ", instance: "
              + instanceName + ", participant config is null");
        }

        InstanceConfig config = new InstanceConfig(currentData);
        config.setInstanceEnabled(enabled);
        return config.getRecord();
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void enablePartition(final boolean enabled,
                              final String clusterName,
                              final String instanceName,
                              final String resourceName,
                              final List<String> partitionNames)
  {
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                   clusterName,
                                   ConfigScopeProperty.PARTICIPANT.toString(),
                                   instanceName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    // check instanceConfig exists
    if (!baseAccessor.exists(path, 0))
    {
      throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }

    // check resource exists
    String idealStatePath =
        PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName, resourceName);

    ZNRecord idealStateRecord = null;
    try
    {
      idealStateRecord = baseAccessor.get(idealStatePath, null, 0);
    }
    catch (ZkNoNodeException e)
    {
      // OK.
    }

    if (idealStateRecord == null)
    {
      throw new HelixException("Cluster: " + clusterName + ", resource: " + resourceName
          + ", ideal state does not exist");
    }

    // check partitions exist. warn if not
    IdealState idealState = new IdealState(idealStateRecord);
    for (String partitionName : partitionNames)
    {
      if ((idealState.getIdealStateMode() == IdealStateModeProperty.AUTO && idealState.getPreferenceList(partitionName) == null)
          || (idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED && idealState.getInstanceStateMap(partitionName) == null))
      {
        logger.warn("Cluster: " + clusterName + ", resource: " + resourceName
            + ", partition: " + partitionName
            + ", partition does not exist in ideal state");
      }
    }

    // update participantConfig
    // could not use ZNRecordUpdater since it doesn't do listField merge/subtract
    baseAccessor.update(path, new DataUpdater<ZNRecord>()
    {
      @Override
      public ZNRecord update(ZNRecord currentData)
      {
        if (currentData == null)
        {
          throw new HelixException("Cluster: " + clusterName + ", instance: "
              + instanceName + ", participant config is null");
        }

        // TODO: merge with InstanceConfig.setInstanceEnabledForPartition
        List<String> list =
            currentData.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
        Set<String> disabledPartitions = new HashSet<String>();
        if (list != null)
        {
          disabledPartitions.addAll(list);
        }

        if (enabled)
        {
          disabledPartitions.removeAll(partitionNames);
        }
        else
        {
          disabledPartitions.addAll(partitionNames);
        }

        list = new ArrayList<String>(disabledPartitions);
        Collections.sort(list);
        currentData.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(),
                                 list);
        return currentData;
      }
    },
                        AccessOption.PERSISTENT);
  }

  @Override
  public void enableCluster(String clusterName, boolean enabled)
  {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    if (enabled)
    {
      accessor.removeProperty(keyBuilder.pause());
    }
    else
    {
      accessor.createProperty(keyBuilder.pause(), new PauseSignal("pause"));
    }
  }

  @Override
  public void resetPartition(String clusterName,
                             String instanceName,
                             String resourceName,
                             List<String> partitionNames)
  {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    // check the instance is alive
    LiveInstance liveInstance =
        accessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null)
    {
      throw new HelixException("Can't reset state for " + resourceName + "/"
          + partitionNames + " on " + instanceName + ", because " + instanceName
          + " is not alive");
    }

    // check resource group exists
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    if (idealState == null)
    {
      throw new HelixException("Can't reset state for " + resourceName + "/"
          + partitionNames + " on " + instanceName + ", because " + resourceName
          + " is not added");
    }

    // check partition exists in resource group
    Set<String> resetPartitionNames = new HashSet<String>(partitionNames);
    if (idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED)
    {
      Set<String> partitions =
          new HashSet<String>(idealState.getRecord().getMapFields().keySet());
      if (!partitions.containsAll(resetPartitionNames))
      {
        throw new HelixException("Can't reset state for " + resourceName + "/"
            + partitionNames + " on " + instanceName + ", because not all "
            + partitionNames + " exist");
      }
    }
    else
    {
      Set<String> partitions =
          new HashSet<String>(idealState.getRecord().getListFields().keySet());
      if (!partitions.containsAll(resetPartitionNames))
      {
        throw new HelixException("Can't reset state for " + resourceName + "/"
            + partitionNames + " on " + instanceName + ", because not all "
            + partitionNames + " exist");
      }
    }

    // check partition is in ERROR state
    String sessionId = liveInstance.getSessionId();
    CurrentState curState =
        accessor.getProperty(keyBuilder.currentState(instanceName,
                                                     sessionId,
                                                     resourceName));
    for (String partitionName : resetPartitionNames)
    {
      if (!curState.getState(partitionName).equals("ERROR"))
      {
        throw new HelixException("Can't reset state for " + resourceName + "/"
            + partitionNames + " on " + instanceName + ", because not all "
            + partitionNames + " are in ERROR state");
      }
    }

    // check stateModelDef exists and get initial state
    String stateModelDef = idealState.getStateModelDefRef();
    StateModelDefinition stateModel =
        accessor.getProperty(keyBuilder.stateModelDef(stateModelDef));
    if (stateModel == null)
    {
      throw new HelixException("Can't reset state for " + resourceName + "/"
          + partitionNames + " on " + instanceName + ", because " + stateModelDef
          + " is NOT found");
    }

    // check there is no pending messages for the partitions exist
    List<Message> messages = accessor.getChildValues(keyBuilder.messages(instanceName));
    for (Message message : messages)
    {
      if (!MessageType.STATE_TRANSITION.toString().equalsIgnoreCase(message.getMsgType())
          || !sessionId.equals(message.getTgtSessionId())
          || !resourceName.equals(message.getResourceName())
          || !resetPartitionNames.contains(message.getPartitionName()))
      {
        continue;
      }

      throw new HelixException("Can't reset state for " + resourceName + "/"
          + partitionNames + " on " + instanceName
          + ", because a pending message exists: " + message);
    }

    String adminName = null;
    try
    {
      adminName = InetAddress.getLocalHost().getCanonicalHostName() + "-ADMIN";
    }
    catch (UnknownHostException e)
    {
      // can ignore it
      logger.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e);
      adminName = "UNKNOWN";
    }

    List<Message> resetMessages = new ArrayList<Message>();
    List<PropertyKey> messageKeys = new ArrayList<PropertyKey>();
    for (String partitionName : resetPartitionNames)
    {
      // send ERROR to initialState message
      String msgId = UUID.randomUUID().toString();
      Message message = new Message(MessageType.STATE_TRANSITION, msgId);
      message.setSrcName(adminName);
      message.setTgtName(instanceName);
      message.setMsgState(MessageState.NEW);
      message.setPartitionName(partitionName);
      message.setResourceName(resourceName);
      message.setTgtSessionId(sessionId);
      message.setStateModelDef(stateModelDef);
      message.setFromState("ERROR");
      message.setToState(stateModel.getInitialState());
      message.setStateModelFactoryName(idealState.getStateModelFactoryName());

      resetMessages.add(message);
      messageKeys.add(keyBuilder.message(instanceName, message.getId()));
    }

    accessor.setChildren(messageKeys, resetMessages);
  }

  @Override
  public void resetInstance(String clusterName, List<String> instanceNames)
  {
    // TODO: not mp-safe
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    List<ExternalView> extViews = accessor.getChildValues(keyBuilder.externalViews());

    Set<String> resetInstanceNames = new HashSet<String>(instanceNames);
    for (String instanceName : resetInstanceNames)
    {
      List<String> resetPartitionNames = new ArrayList<String>();
      for (ExternalView extView : extViews)
      {
        Map<String, Map<String, String>> stateMap = extView.getRecord().getMapFields();
        for (String partitionName : stateMap.keySet())
        {
          Map<String, String> instanceStateMap = stateMap.get(partitionName);

          if (instanceStateMap.containsKey(instanceName)
              && instanceStateMap.get(instanceName).equals("ERROR"))
          {
            resetPartitionNames.add(partitionName);
          }
        }
        resetPartition(clusterName,
                       instanceName,
                       extView.getResourceName(),
                       resetPartitionNames);
      }
    }
  }

  @Override
  public void resetResource(String clusterName, List<String> resourceNames)
  {
    // TODO: not mp-safe
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    List<ExternalView> extViews = accessor.getChildValues(keyBuilder.externalViews());

    Set<String> resetResourceNames = new HashSet<String>(resourceNames);
    for (ExternalView extView : extViews)
    {
      if (!resetResourceNames.contains(extView.getResourceName()))
      {
        continue;
      }

      // instanceName -> list of resetPartitionNames
      Map<String, List<String>> resetPartitionNames = new HashMap<String, List<String>>();

      Map<String, Map<String, String>> stateMap = extView.getRecord().getMapFields();
      for (String partitionName : stateMap.keySet())
      {
        Map<String, String> instanceStateMap = stateMap.get(partitionName);
        for (String instanceName : instanceStateMap.keySet())
        {
          if (instanceStateMap.get(instanceName).equals("ERROR"))
          {
            if (!resetPartitionNames.containsKey(instanceName))
            {
              resetPartitionNames.put(instanceName, new ArrayList<String>());
            }
            resetPartitionNames.get(instanceName).add(partitionName);
          }
        }
      }

      for (String instanceName : resetPartitionNames.keySet())
      {
        resetPartition(clusterName,
                       instanceName,
                       extView.getResourceName(),
                       resetPartitionNames.get(instanceName));
      }
    }
  }

  @Override
  public void addCluster(String clusterName, boolean overwritePrevRecord)
  {
    String root = "/" + clusterName;
    String path;

    // TODO For ease of testing only, should remove later
    if (_zkClient.exists(root))
    {
      logger.warn("Root directory exists.Cleaning the root directory:" + root
          + " overwritePrevRecord: " + overwritePrevRecord);
      if (overwritePrevRecord)
      {
        _zkClient.deleteRecursive(root);
      }
      else
      {
        throw new HelixException("Cluster " + clusterName + " already exists");
      }
    }

    _zkClient.createPersistent(root);

    // IDEAL STATE
    _zkClient.createPersistent(HelixUtil.getIdealStatePath(clusterName));
    // CONFIGURATIONS
    // _zkClient.createPersistent(HelixUtil.getConfigPath(clusterName));
    path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                   clusterName,
                                   ConfigScopeProperty.CLUSTER.toString(),
                                   clusterName);
    _zkClient.createPersistent(path, true);
    _zkClient.writeData(path, new ZNRecord(clusterName));
    path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                   clusterName,
                                   ConfigScopeProperty.PARTICIPANT.toString());
    _zkClient.createPersistent(path);
    path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                   clusterName,
                                   ConfigScopeProperty.RESOURCE.toString());
    _zkClient.createPersistent(path);
    // PROPERTY STORE
    path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);
    _zkClient.createPersistent(path);
    // LIVE INSTANCES
    _zkClient.createPersistent(HelixUtil.getLiveInstancesPath(clusterName));
    // MEMBER INSTANCES
    _zkClient.createPersistent(HelixUtil.getMemberInstancesPath(clusterName));
    // External view
    _zkClient.createPersistent(HelixUtil.getExternalViewPath(clusterName));
    // State model definition
    _zkClient.createPersistent(HelixUtil.getStateModelDefinitionPath(clusterName));

    // controller
    _zkClient.createPersistent(HelixUtil.getControllerPath(clusterName));
    path = PropertyPathConfig.getPath(PropertyType.HISTORY, clusterName);
    final ZNRecord emptyHistory = new ZNRecord(PropertyType.HISTORY.toString());
    final List<String> emptyList = new ArrayList<String>();
    emptyHistory.setListField(clusterName, emptyList);
    _zkClient.createPersistent(path, emptyHistory);

    path = PropertyPathConfig.getPath(PropertyType.MESSAGES_CONTROLLER, clusterName);
    _zkClient.createPersistent(path);

    path = PropertyPathConfig.getPath(PropertyType.STATUSUPDATES_CONTROLLER, clusterName);
    _zkClient.createPersistent(path);

    path = PropertyPathConfig.getPath(PropertyType.ERRORS_CONTROLLER, clusterName);
    _zkClient.createPersistent(path);
  }

  @Override
  public List<String> getInstancesInCluster(String clusterName)
  {
    String memberInstancesPath = HelixUtil.getMemberInstancesPath(clusterName);
    return _zkClient.getChildren(memberInstancesPath);
  }

  @Override
  public void addResource(String clusterName,
                          String resourceName,
                          int partitions,
                          String stateModelRef)
  {
    addResource(clusterName,
                resourceName,
                partitions,
                stateModelRef,
                IdealStateModeProperty.AUTO.toString(),
                0);
  }

  @Override
  public void addResource(String clusterName,
                          String resourceName,
                          int partitions,
                          String stateModelRef,
                          String idealStateMode)
  {
    addResource(clusterName, resourceName, partitions, stateModelRef, idealStateMode, 0);
  }

  @Override
  public void addResource(String clusterName, 
                          String resourceName,
                          IdealState idealstate)
  {
    String stateModelRef = idealstate.getStateModelDefRef();
    String stateModelDefPath =
      PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS,
                                 clusterName,
                                 stateModelRef);
  if (!_zkClient.exists(stateModelDefPath))
  {
    throw new HelixException("State model " + stateModelRef
        + " not found in the cluster STATEMODELDEFS path");
  }

  String idealStatePath = HelixUtil.getIdealStatePath(clusterName);
  String dbIdealStatePath = idealStatePath + "/" + resourceName;
  if (_zkClient.exists(dbIdealStatePath))
  {
    throw new HelixException("Skip the operation. DB ideal state directory exists:"
        + dbIdealStatePath);
  }

  ZKUtil.createChildren(_zkClient, idealStatePath, idealstate.getRecord()); 
  }
  
  @Override
  public void addResource(String clusterName,
                          String resourceName,
                          int partitions,
                          String stateModelRef,
                          String idealStateMode,
                          int bucketSize)
  {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    IdealStateModeProperty mode = IdealStateModeProperty.AUTO;
    try
    {
      mode = IdealStateModeProperty.valueOf(idealStateMode);
    }
    catch (Exception e)
    {
      logger.error("", e);
    }
    IdealState idealState = new IdealState(resourceName);
    idealState.setNumPartitions(partitions);
    idealState.setStateModelDefRef(stateModelRef);
    idealState.setIdealStateMode(mode.toString());
    idealState.setReplicas("" + 0);
    idealState.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);

    if (bucketSize > 0)
    {
      idealState.setBucketSize(bucketSize);
    }
    addResource(clusterName, resourceName, idealState);

  }

  @Override
  public List<String> getClusters()
  {
    List<String> zkToplevelPathes = _zkClient.getChildren("/");
    List<String> result = new ArrayList<String>();
    for (String pathName : zkToplevelPathes)
    {
      if (ZKUtil.isClusterSetup(pathName, _zkClient))
      {
        result.add(pathName);
      }
    }
    return result;
  }

  @Override
  public List<String> getResourcesInCluster(String clusterName)
  {
    return _zkClient.getChildren(HelixUtil.getIdealStatePath(clusterName));
  }

  @Override
  public IdealState getResourceIdealState(String clusterName, String dbName)
  {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.idealStates(dbName));
  }

  @Override
  public void setResourceIdealState(String clusterName,
                                    String dbName,
                                    IdealState idealState)
  {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.setProperty(keyBuilder.idealStates(dbName), idealState);
  }

  @Override
  public ExternalView getResourceExternalView(String clusterName, String resourceName)
  {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    return accessor.getProperty(keyBuilder.externalView(resourceName));
  }

  @Override
  public void addStateModelDef(String clusterName,
                               String stateModelDef,
                               StateModelDefinition stateModel)
  {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }
    String stateModelDefPath = HelixUtil.getStateModelDefinitionPath(clusterName);
    String stateModelPath = stateModelDefPath + "/" + stateModelDef;
    if (_zkClient.exists(stateModelPath))
    {
      logger.warn("Skip the operation.State Model directory exists:" + stateModelPath);
      throw new HelixException("State model path " + stateModelPath + " already exists.");
    }

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef(stateModel.getId()), stateModel);
  }

  @Override
  public void dropResource(String clusterName, String resourceName)
  {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.removeProperty(keyBuilder.idealStates(resourceName));
  }

  @Override
  public List<String> getStateModelDefs(String clusterName)
  {
    return _zkClient.getChildren(HelixUtil.getStateModelDefinitionPath(clusterName));
  }

  @Override
  public StateModelDefinition getStateModelDef(String clusterName, String stateModelName)
  {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    return accessor.getProperty(keyBuilder.stateModelDef(stateModelName));
  }

  @Override
  public void addStat(String clusterName, final String statName)
  {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    String persistentStatsPath =
        PropertyPathConfig.getPath(PropertyType.PERSISTENTSTATS, clusterName);
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    baseAccessor.update(persistentStatsPath, new DataUpdater<ZNRecord>()
    {

      @Override
      public ZNRecord update(ZNRecord statsRec)
      {
        if (statsRec == null)
        {
          // TODO: fix naming of this record, if it matters
          statsRec = new ZNRecord(PersistentStats.nodeName);
        }

        Map<String, Map<String, String>> currStatMap = statsRec.getMapFields();
        Map<String, Map<String, String>> newStatMap = StatsHolder.parseStat(statName);
        for (String newStat : newStatMap.keySet())
        {
          if (!currStatMap.containsKey(newStat))
          {
            currStatMap.put(newStat, newStatMap.get(newStat));
          }
        }
        statsRec.setMapFields(currStatMap);

        return statsRec;
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void addAlert(final String clusterName, final String alertName)
  {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    String alertsPath = PropertyPathConfig.getPath(PropertyType.ALERTS, clusterName);

    baseAccessor.update(alertsPath, new DataUpdater<ZNRecord>()
    {

      @Override
      public ZNRecord update(ZNRecord alertsRec)
      {
        if (alertsRec == null)
        {
          // TODO: fix naming of this record, if it matters
          alertsRec = new ZNRecord(Alerts.nodeName);

        }

        Map<String, Map<String, String>> currAlertMap = alertsRec.getMapFields();
        StringBuilder newStatName = new StringBuilder();
        Map<String, String> newAlertMap = new HashMap<String, String>();

        // use AlertsHolder to get map of new stats and map for this alert
        AlertsHolder.parseAlert(alertName, newStatName, newAlertMap);

        // add stat
        addStat(clusterName, newStatName.toString());

        // add alert
        currAlertMap.put(alertName, newAlertMap);

        alertsRec.setMapFields(currAlertMap);

        return alertsRec;
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void dropCluster(String clusterName)
  {
    logger.info("Deleting cluster " + clusterName);
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    String root = "/" + clusterName;
    if (accessor.getChildNames(keyBuilder.liveInstances()).size() > 0)
    {
      throw new HelixException("There are still live instances in the cluster, shut them down first.");
    }

    if (accessor.getProperty(keyBuilder.controllerLeader()) != null)
    {
      throw new HelixException("There are still LEADER in the cluster, shut them down first.");
    }

    _zkClient.deleteRecursive(root);
  }

  @Override
  public void dropStat(String clusterName, final String statName)
  {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    String persistentStatsPath =
        PropertyPathConfig.getPath(PropertyType.PERSISTENTSTATS, clusterName);
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    baseAccessor.update(persistentStatsPath, new DataUpdater<ZNRecord>()
    {

      @Override
      public ZNRecord update(ZNRecord statsRec)
      {
        if (statsRec == null)
        {
          throw new HelixException("No stats record in ZK, nothing to drop");
        }

        Map<String, Map<String, String>> currStatMap = statsRec.getMapFields();
        Map<String, Map<String, String>> newStatMap = StatsHolder.parseStat(statName);

        // delete each stat from stat map
        for (String newStat : newStatMap.keySet())
        {
          if (currStatMap.containsKey(newStat))
          {
            currStatMap.remove(newStat);
          }
        }
        statsRec.setMapFields(currStatMap);

        return statsRec;
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void dropAlert(String clusterName, final String alertName)
  {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    String alertsPath = PropertyPathConfig.getPath(PropertyType.ALERTS, clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    if (!baseAccessor.exists(alertsPath, 0))
    {
      throw new HelixException("No alerts node in ZK, nothing to drop");
    }

    baseAccessor.update(alertsPath, new DataUpdater<ZNRecord>()
    {
      @Override
      public ZNRecord update(ZNRecord alertsRec)
      {
        if (alertsRec == null)
        {
          throw new HelixException("No alerts record in ZK, nothing to drop");
        }

        Map<String, Map<String, String>> currAlertMap = alertsRec.getMapFields();
        currAlertMap.remove(alertName);
        alertsRec.setMapFields(currAlertMap);

        return alertsRec;
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void addClusterToGrandCluster(String clusterName, String grandCluster)
  {
    if (!ZKUtil.isClusterSetup(grandCluster, _zkClient))
    {
      throw new HelixException("Grand cluster " + grandCluster + " is not setup yet");
    }

    if (!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new HelixException("Cluster " + clusterName + " is not setup yet");
    }

    IdealState idealState = new IdealState(clusterName);

    idealState.setNumPartitions(1);
    idealState.setStateModelDefRef("LeaderStandby");

    List<String> controllers = getInstancesInCluster(grandCluster);
    if (controllers.size() == 0)
    {
      throw new HelixException("Grand cluster " + grandCluster + " has no instances");
    }
    idealState.setReplicas(Integer.toString(controllers.size()));
    Collections.shuffle(controllers);
    idealState.getRecord().setListField(clusterName, controllers);
    idealState.setPartitionState(clusterName, controllers.get(0), "LEADER");
    for (int i = 1; i < controllers.size(); i++)
    {
      idealState.setPartitionState(clusterName, controllers.get(i), "STANDBY");
    }

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(grandCluster, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.setProperty(keyBuilder.idealStates(idealState.getResourceName()), idealState);
  }

  @Override
  public void setConfig(ConfigScope scope, Map<String, String> properties)
  {
    for (String key : properties.keySet())
    {
      _configAccessor.set(scope, key, properties.get(key));
    }
  }

  @Override
  public Map<String, String> getConfig(ConfigScope scope, Set<String> keys)
  {
    Map<String, String> properties = new TreeMap<String, String>();

    if (keys == null)
    {
      // read all simple fields

    }
    else
    {
      for (String key : keys)
      {
        String value = _configAccessor.get(scope, key);
        if (value == null)
        {
          logger.error("Config doesn't exist for key: " + key);
          continue;
        }
        properties.put(key, value);
      }
    }

    return properties;
  }

  @Override
  public List<String> getConfigKeys(ConfigScopeProperty scope,
                                    String clusterName,
                                    String... keys)
  {
    return _configAccessor.getKeys(scope, clusterName, keys);
  }

  @Override
  public void removeConfig(ConfigScope scope, Set<String> keys)
  {
    for (String key : keys)
    {
      _configAccessor.remove(scope, key);
    }
  }

  @Override
  public void rebalance(String clusterName, String resourceName, int replica)
  {
    rebalance(clusterName, resourceName, replica, resourceName);
  }

  void rebalance(String clusterName, String resourceName, int replica, String keyPrefix)
  {
    List<String> InstanceNames = getInstancesInCluster(clusterName);

    // ensure we get the same idealState with the same set of instances
    Collections.sort(InstanceNames);

    IdealState idealState = getResourceIdealState(clusterName, resourceName);
    if (idealState == null)
    {
      throw new HelixException("Resource: " + resourceName + " has NOT been added yet");
    }

    idealState.setReplicas(Integer.toString(replica));
    int partitions = idealState.getNumPartitions();
    String stateModelName = idealState.getStateModelDefRef();
    StateModelDefinition stateModDef = getStateModelDef(clusterName, stateModelName);

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

      // for now keep mapField in AUTO mode and remove listField in CUSTOMIZED mode
      if (idealState.getIdealStateMode() == IdealStateModeProperty.AUTO)
      {
        idealState.getRecord().setListFields(newIdealState.getListFields());
        idealState.getRecord().setMapFields(newIdealState.getMapFields());
      }
      if (idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED)
      {
        idealState.getRecord().setMapFields(newIdealState.getMapFields());
      }
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
    setResourceIdealState(clusterName, resourceName, idealState);
  }

  @Override
  public void addIdealState(String clusterName, String resourceName, String idealStateFile) throws IOException
  {
    ZNRecord idealStateRecord =
        (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(idealStateFile)));
    if (idealStateRecord.getId() == null
        || !idealStateRecord.getId().equals(resourceName))
    {
      throw new IllegalArgumentException("ideal state must have same id as resource name");
    }
    setResourceIdealState(clusterName, resourceName, new IdealState(idealStateRecord));
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

  public void addStateModelDef(String clusterName,
                               String stateModelDefName,
                               String stateModelDefFile) throws IOException
  {
    ZNRecord record =
        (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(stateModelDefFile)));
    if (record == null || record.getId() == null
        || !record.getId().equals(stateModelDefName))
    {
      throw new IllegalArgumentException("state model definition must have same id as state model def name");
    }
    addStateModelDef(clusterName, stateModelDefName, new StateModelDefinition(record));

  }

  public void addMessageConstraint(String clusterName,
                                   final String constraintId,
                                   final Map<String, String> constraints)
  {
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient);

    Builder keyBuilder = new Builder(clusterName);
    String path = keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()).getPath();

    baseAccessor.update(path, new DataUpdater<ZNRecord>()
    {
      @Override
      public ZNRecord update(ZNRecord currentData)
      {
        if (currentData == null)
        {
          currentData = new ZNRecord(ConstraintType.MESSAGE_CONSTRAINT.toString());
        }

        Map<String, String> map = currentData.getMapField(constraintId);
        if (map == null)
        {
          map = new TreeMap<String, String>();
          currentData.setMapField(constraintId, map);
        } else
        {
          logger.warn("Overwrite existing constraint " + constraintId + ": " + map);
        }

        for (String key : constraints.keySet())
        {
          // make sure contraint attribute is valid
          ConstraintAttribute attr = ConstraintAttribute.valueOf(key.toUpperCase());

          map.put(attr.toString(), constraints.get(key));
        }
        
        return currentData;
      }
    }, AccessOption.PERSISTENT);
  }

  
}
