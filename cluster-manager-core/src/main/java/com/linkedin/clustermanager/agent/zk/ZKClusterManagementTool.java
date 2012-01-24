package com.linkedin.clustermanager.agent.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.IdealState.IdealStateModeProperty;
import com.linkedin.clustermanager.model.IdealState.IdealStateProperty;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.util.CMUtil;
import com.linkedin.clustermanager.alerts.AlertsHolder;
import com.linkedin.clustermanager.alerts.StatsHolder;
import com.linkedin.clustermanager.model.Alerts;
import com.linkedin.clustermanager.model.PersistentStats;

public class ZKClusterManagementTool implements ClusterManagementService
{

  private final ZkClient _zkClient;

  private static Logger logger = Logger
      .getLogger(ZKClusterManagementTool.class);

  public ZKClusterManagementTool(ZkClient zkClient)
  {
    _zkClient = zkClient;
  }

  @Override
  public void addInstance(String clusterName, InstanceConfig instanceConfig)
  {
    if(!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new ClusterManagerException("cluster " + clusterName
         + " is not setup yet");
    }
    String instanceConfigsPath = CMUtil.getConfigPath(clusterName);
    String nodeId = instanceConfig.getId();
    String instanceConfigPath = instanceConfigsPath + "/" + nodeId;

    if (_zkClient.exists(instanceConfigPath))
    {
      throw new ClusterManagerException("Node " + nodeId
          + " already exists in cluster " + clusterName);
    }

    ZKUtil.createChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord());

    _zkClient
        .createPersistent(CMUtil.getMessagePath(clusterName, nodeId), true);
    _zkClient.createPersistent(
        CMUtil.getCurrentStateBasePath(clusterName, nodeId), true);
    _zkClient.createPersistent(CMUtil.getErrorsPath(clusterName, nodeId), true);
    _zkClient.createPersistent(
        CMUtil.getStatusUpdatesPath(clusterName, nodeId), true);
  }

  @Override
  public void dropInstance(String clusterName, InstanceConfig instanceConfig)
  {
    String instanceConfigsPath = CMUtil.getConfigPath(clusterName);
    String nodeId = instanceConfig.getId();
    String instanceConfigPath = instanceConfigsPath + "/" + nodeId;
    String instancePath = CMUtil.getInstancePath(clusterName, nodeId);

    if (!_zkClient.exists(instanceConfigPath))
    {
      throw new ClusterManagerException("Node " + nodeId
          + " does not exist in config for cluster " + clusterName);
    }

    if (!_zkClient.exists(instancePath))
    {
      throw new ClusterManagerException("Node " + nodeId
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
    String instanceConfigsPath = CMUtil.getConfigPath(clusterName);
    String instanceConfigPath = instanceConfigsPath + "/" + instanceName;

    if (!_zkClient.exists(instanceConfigPath))
    {
      throw new ClusterManagerException("instance" + instanceName
          + " does not exist in cluster " + clusterName);
    }

    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    return accessor.getProperty(InstanceConfig.class, PropertyType.CONFIGS, instanceName);
  }

  @Override
  public void enableInstance(String clusterName, String instanceName,
      boolean enabled)
  {
    String targetPath = PropertyPathConfig.getPath(
        PropertyType.CONFIGS, clusterName, instanceName);

    if (_zkClient.exists(targetPath))
    {
      ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
      InstanceConfig nodeConfig = accessor.getProperty(InstanceConfig.class,
                                                       PropertyType.CONFIGS,
                                                       instanceName);

      nodeConfig.setInstanceEnabled(enabled);
      accessor.setProperty(PropertyType.CONFIGS, nodeConfig, instanceName);
    } else
    {
      throw new ClusterManagerException("Cluster " + clusterName
          + ", instance " + instanceName + " does not exist");
    }
  }

  @Override
  public void enablePartition(String clusterName, String instanceName, String partition,
      boolean enabled)
  {
    String path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, instanceName);
    if (_zkClient.exists(path))
    {
      ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
      InstanceConfig nodeConfig = accessor.getProperty(InstanceConfig.class,
                                                       PropertyType.CONFIGS,
                                                       instanceName);

      nodeConfig.setInstanceEnabledForResource(partition, enabled);
      accessor.setProperty(PropertyType.CONFIGS, nodeConfig, instanceName);
    }
    else
    {
      throw new ClusterManagerException("Cluster " + clusterName
          + ", instance " + instanceName + " does not exist");
    }
  }

  @Override
  public void resetPartition(String clusterName, String instanceName, String resourceGroupName,
                             String partition)
  {
    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    LiveInstance liveInstance = accessor.getProperty(LiveInstance.class, PropertyType.LIVEINSTANCES, instanceName);

    if (liveInstance == null)
    {
      throw new IllegalArgumentException("Can't reset resource state " + partition + ", because instance is not alive");
    }

    String sessionId = liveInstance.getSessionId();
    CurrentState curState = accessor.getProperty(CurrentState.class,
                                                 PropertyType.CURRENTSTATES,
                                                 instanceName,
                                                 sessionId,
                                                 resourceGroupName);
    curState.resetState(partition);
    accessor.setProperty(PropertyType.CURRENTSTATES, curState, instanceName, sessionId, resourceGroupName);
  }

  @Override
  public void addCluster(String clusterName, boolean overwritePrevRecord)
  {
    // TODO Auto-generated method stub
    String root = "/" + clusterName;

    // TODO For ease of testing only, should remove later
    if (_zkClient.exists(root))
    {
      logger.warn("Root directory exists.Cleaning the root directory:" + root
          + " overwritePrevRecord: " + overwritePrevRecord);
      if (overwritePrevRecord)
      {
        _zkClient.deleteRecursive(root);
      } else
      {
        throw new ClusterManagerException("Cluster " + clusterName
            + " already exists");
      }
    }

    _zkClient.createPersistent(root);

    // IDEAL STATE
    _zkClient.createPersistent(CMUtil.getIdealStatePath(clusterName));
    // CONFIGURATIONS
    _zkClient.createPersistent(CMUtil.getConfigPath(clusterName));
    // LIVE INSTANCES
    _zkClient.createPersistent(CMUtil.getLiveInstancesPath(clusterName));
    // MEMBER INSTANCES
    _zkClient.createPersistent(CMUtil.getMemberInstancesPath(clusterName));
    // External view
    _zkClient.createPersistent(CMUtil.getExternalViewPath(clusterName));
    // State model definition
    _zkClient.createPersistent(CMUtil.getStateModelDefinitionPath(clusterName));

    // controller
    _zkClient.createPersistent(CMUtil.getControllerPath(clusterName));
    String path = PropertyPathConfig.getPath(PropertyType.HISTORY, clusterName);
    final ZNRecord emptyHistory = new ZNRecord(PropertyType.HISTORY.toString());
    final List<String> emptyList = new ArrayList<String>();
    emptyHistory.setListField(clusterName, emptyList);
    _zkClient.createPersistent(path, emptyHistory);

    path = PropertyPathConfig.getPath(PropertyType.MESSAGES_CONTROLLER,
        clusterName);
    _zkClient.createPersistent(path);

    path = PropertyPathConfig.getPath(PropertyType.STATUSUPDATES_CONTROLLER,
        clusterName);
    _zkClient.createPersistent(path);

    path = PropertyPathConfig.getPath(PropertyType.ERRORS_CONTROLLER,
        clusterName);
    _zkClient.createPersistent(path);
  }

  @Override
  public List<String> getInstancesInCluster(String clusterName)
  {
    String memberInstancesPath = CMUtil.getMemberInstancesPath(clusterName);
    return _zkClient.getChildren(memberInstancesPath);
  }

  @Override
  public void addResourceGroup(String clusterName, String dbName,
      int partitions, String stateModelRef)
  {
    addResourceGroup(clusterName, dbName, partitions, stateModelRef,
        IdealStateModeProperty.AUTO.toString());
  }

  @Override
  public void addResourceGroup(String clusterName, String dbName,
      int partitions, String stateModelRef, String idealStateMode)
  {
    if(!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new ClusterManagerException("cluster " + clusterName
         + " is not setup yet");
    }
    ZNRecord idealState = new ZNRecord(dbName);
    idealState.setSimpleField(IdealStateProperty.RESOURCES.toString(), String.valueOf(partitions));
    idealState.setSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString(), stateModelRef);
    idealState.setSimpleField(IdealStateProperty.IDEAL_STATE_MODE.toString(), idealStateMode);
    idealState.setSimpleField(IdealStateProperty.REPLICAS.toString(), 0+"");

    String stateModelDefPath = PropertyPathConfig.getPath(
        PropertyType.STATEMODELDEFS, clusterName, stateModelRef);
    if (!_zkClient.exists(stateModelDefPath))
    {
      throw new ClusterManagerException("State model " + stateModelRef
          + " not found in the cluster STATEMODELDEFS path");
    }

    String idealStatePath = CMUtil.getIdealStatePath(clusterName);
    String dbIdealStatePath = idealStatePath + "/" + dbName;
    if (_zkClient.exists(dbIdealStatePath))
    {
      logger.warn("Skip the operation. DB ideal state directory exists:"
          + dbIdealStatePath);
      return;
    }
    ZKUtil.createChildren(_zkClient, idealStatePath, idealState);
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
  public List<String> getResourceGroupsInCluster(String clusterName)
  {
    return _zkClient.getChildren(CMUtil.getIdealStatePath(clusterName));
  }

  @Override
  public IdealState getResourceGroupIdealState(String clusterName, String dbName)
  {
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    return accessor.getProperty(IdealState.class, PropertyType.IDEALSTATES, dbName);
  }

  @Override
  public void setResourceGroupIdealState(String clusterName, String dbName,
      IdealState idealState)
  {
    new ZKDataAccessor(clusterName, _zkClient).setProperty(PropertyType.IDEALSTATES,
                                                           idealState,
                                                           dbName);
  }

  @Override
  public ExternalView getResourceGroupExternalView(String clusterName,
      String resourceGroup)
  {
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    return accessor.getProperty(ExternalView.class, PropertyType.EXTERNALVIEW, resourceGroup);
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDef,
      StateModelDefinition stateModel)
  {
    if(!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new ClusterManagerException("cluster " + clusterName
         + " is not setup yet");
    }
    String stateModelDefPath = CMUtil.getStateModelDefinitionPath(clusterName);
    String stateModelPath = stateModelDefPath + "/" + stateModelDef;
    if (_zkClient.exists(stateModelPath))
    {
      logger.warn("Skip the operation.State Model directory exists:"
          + stateModelPath);
      throw new ClusterManagerException("State model path " + stateModelPath
          + " already exists.");
    }

    ZKUtil.createChildren(_zkClient, stateModelDefPath, stateModel.getRecord());
    new ZKDataAccessor(clusterName, _zkClient).setProperty(
        PropertyType.STATEMODELDEFS, stateModel, stateModel.getId());
  }

  @Override
  public void dropResourceGroup(String clusterName, String resourceGroup)
  {
    new ZKDataAccessor(clusterName, _zkClient).removeProperty(
        PropertyType.IDEALSTATES, resourceGroup);
  }

  @Override
  public List<String> getStateModelDefs(String clusterName)
  {
    return _zkClient.getChildren(CMUtil
        .getStateModelDefinitionPath(clusterName));
  }

  @Override
  public StateModelDefinition getStateModelDef(String clusterName, String stateModelName)
  {
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    return accessor.getProperty(StateModelDefinition.class,
                                PropertyType.STATEMODELDEFS,
                                stateModelName);
  }

  @Override
  public void addStat(String clusterName, String statName)
  {
    if(!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new ClusterManagerException("cluster " + clusterName
          + " is not setup yet");
    }
    
    String persistentStatsPath = CMUtil.getPersistentStatsPath(clusterName);
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    if (!_zkClient.exists(persistentStatsPath)) {      
      //ZKUtil.createChildren(_zkClient, persistentStatsPath, statsRec);
      _zkClient.createPersistent(persistentStatsPath);
    }
    ZNRecord statsRec = accessor.getProperty(PropertyType.PERSISTENTSTATS);
    if (statsRec == null) {
      statsRec = new ZNRecord(PersistentStats.nodeName); //TODO: fix naming of this record, if it matters
    }
    
    Map<String,Map<String,String>> currStatMap = statsRec.getMapFields();
    Map<String,Map<String,String>> newStatMap = StatsHolder.parseStat(statName);
    for (String newStat : newStatMap.keySet()) {
     if (!currStatMap.containsKey(newStat)) {
       currStatMap.put(newStat, newStatMap.get(newStat));
     }
    }
    statsRec.setMapFields(currStatMap);
    accessor.setProperty(PropertyType.PERSISTENTSTATS, statsRec); 
  }

  @Override
  public void addAlert(String clusterName, String alertName) 
  {
    if(!ZKUtil.isClusterSetup(clusterName, _zkClient))
    {
      throw new ClusterManagerException("cluster " + clusterName
          + " is not setup yet");
    }
    
    String alertsPath = CMUtil.getAlertsPath(clusterName);
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    if (!_zkClient.exists(alertsPath)) {      
      //ZKUtil.createChildren(_zkClient, alertsPath, alertsRec);
      _zkClient.createPersistent(alertsPath);
    }
    ZNRecord alertsRec = accessor.getProperty(PropertyType.ALERTS);
    if (alertsRec == null)
    {
      alertsRec = new ZNRecord(Alerts.nodeName); //TODO: fix naming of this record, if it matters
    }
    
    Map<String,Map<String,String>> currAlertMap = alertsRec.getMapFields();
    StringBuilder newStatName = new StringBuilder();
    Map<String,String> newAlertMap = new HashMap<String,String>();
    //use AlertsHolder to get map of new stats and map for this alert
    AlertsHolder.parseAlert(alertName, newStatName, newAlertMap);
    
    //add stat
    addStat(clusterName, newStatName.toString());
    //add alert
    currAlertMap.put(alertName, newAlertMap);
    
    alertsRec.setMapFields(currAlertMap);
    accessor.setProperty(PropertyType.ALERTS, alertsRec); 
  }

  @Override
  public void dropCluster(String clusterName)
  {
    logger.info("Deleting cluster "+clusterName);
    String root = "/" + clusterName;
    _zkClient.deleteRecursive(root);
  }  
}
