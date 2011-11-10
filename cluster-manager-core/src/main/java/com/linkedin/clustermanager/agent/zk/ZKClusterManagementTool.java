package com.linkedin.clustermanager.agent.zk;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.IdealStateConfigProperty;
import com.linkedin.clustermanager.ClusterDataAccessor.InstanceConfigProperty;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.util.CMUtil;

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
  public void addInstance(String clusterName, ZNRecord instanceConfig)
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

    ZKUtil.createChildren(_zkClient, instanceConfigsPath, instanceConfig);

    _zkClient
        .createPersistent(CMUtil.getMessagePath(clusterName, nodeId), true);
    _zkClient.createPersistent(
        CMUtil.getCurrentStateBasePath(clusterName, nodeId), true);
    _zkClient.createPersistent(CMUtil.getErrorsPath(clusterName, nodeId), true);
    _zkClient.createPersistent(
        CMUtil.getStatusUpdatesPath(clusterName, nodeId), true);
  }

  @Override
  public void dropInstance(String clusterName, ZNRecord instanceConfig) 
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
    ZKUtil.dropChildren(_zkClient, instanceConfigsPath, instanceConfig);

    // delete instance path
    _zkClient.deleteRecursive(instancePath);
  }

  @Override
  public ZNRecord getInstanceConfig(String clusterName, String instanceName)
  {
    String instanceConfigsPath = CMUtil.getConfigPath(clusterName);
    String instanceConfigPath = instanceConfigsPath + "/" + instanceName;

    if (!_zkClient.exists(instanceConfigPath))
    {
      throw new ClusterManagerException("instance" + instanceName
          + " does not exist in cluster " + clusterName);
    }

    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    return accessor.getProperty(PropertyType.CONFIGS, instanceName);
  }

  @Override
  public void enableInstance(String clusterName, String instanceName,
      boolean enable)
  {
    String clusterPropertyPath = PropertyPathConfig.getPath(
        PropertyType.CONFIGS, clusterName);
    String targetPath = clusterPropertyPath + "/" + instanceName;

    if (_zkClient.exists(targetPath))
    {
      ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
      ZNRecord nodeConfig = accessor.getProperty(PropertyType.CONFIGS,
          instanceName);

      nodeConfig.setSimpleField(InstanceConfigProperty.ENABLED.toString(),
          enable + "");
      accessor.setProperty(PropertyType.CONFIGS, nodeConfig, instanceName);
    } else
    {
      throw new ClusterManagerException("Cluster " + clusterName
          + ", instance " + instanceName + " does not exist");
    }
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
        IdealStateConfigProperty.AUTO.toString());
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
    idealState.setSimpleField("partitions", String.valueOf(partitions));
    idealState.setSimpleField("state_model_def_ref", stateModelRef);
    idealState.setSimpleField("ideal_state_mode", idealStateMode);

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
  public ZNRecord getResourceGroupIdealState(String clusterName, String dbName)
  {
    return new ZKDataAccessor(clusterName, _zkClient).getProperty(
        PropertyType.IDEALSTATES, dbName);
  }

  @Override
  public void setResourceGroupIdealState(String clusterName, String dbName,
      ZNRecord idealState)
  {
    new ZKDataAccessor(clusterName, _zkClient).setProperty(
        PropertyType.IDEALSTATES, idealState, dbName);
  }

  @Override
  public ZNRecord getResourceGroupExternalView(String clusterName,
      String resourceGroup)
  {
    return new ZKDataAccessor(clusterName, _zkClient).getProperty(
        PropertyType.EXTERNALVIEW, resourceGroup);
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDef,
      ZNRecord record)
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

    ZKUtil.createChildren(_zkClient, stateModelDefPath, record);
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
  public ZNRecord getStateModelDef(String clusterName, String stateModelName)
  {
    return new ZKDataAccessor(clusterName, _zkClient).getProperty(
        PropertyType.STATEMODELDEFS, stateModelName);
  }
}
