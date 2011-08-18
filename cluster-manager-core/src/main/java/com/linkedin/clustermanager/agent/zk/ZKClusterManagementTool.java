package com.linkedin.clustermanager.agent.zk;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstanceConfigProperty;
import com.linkedin.clustermanager.util.CMUtil;

public class ZKClusterManagementTool implements ClusterManagementService
{

  private ZkClient _zkClient;

  private static Logger logger = Logger
      .getLogger(ZKClusterManagementTool.class);

  public ZKClusterManagementTool(ZkClient zkClient)
  {
    _zkClient = zkClient;
  }

  @Override
  public void addNode(String clusterName, ZNRecord nodeConfig)
  {
    String instanceConfigsPath = CMUtil.getConfigPath(clusterName);
    String nodeId = nodeConfig.getId();
    String instanceConfigPath = instanceConfigsPath + "/" + nodeId;

    if (_zkClient.exists(instanceConfigPath))
    {
      throw new ClusterManagerException("Node " + nodeId
          + " already exists in cluster " + clusterName);
    }

    ZKUtil.createChildren(_zkClient, instanceConfigsPath, nodeConfig);

    _zkClient
        .createPersistent(CMUtil.getMessagePath(clusterName, nodeId), true);
    _zkClient.createPersistent(CMUtil.getCurrentStateBasePath(clusterName, nodeId),
        true);
    _zkClient.createPersistent(CMUtil.getErrorsPath(clusterName, nodeId), true);
    _zkClient.createPersistent(
        CMUtil.getStatusUpdatesPath(clusterName, nodeId), true);
  }

  public ZNRecord getNodeConfig(String nodeId)
  {
    return null;
  }

  @Override
  public void enableInstance(String clusterName, String instanceName,
      boolean enable)
  {
    String clusterPropertyPath = CMUtil.getClusterPropertyPath(clusterName,
        ClusterPropertyType.CONFIGS);
    String targetPath = clusterPropertyPath + "/" + instanceName;

    if (_zkClient.exists(targetPath))
    {
      ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
      ZNRecord nodeConfig = accessor.getClusterProperty(
          ClusterPropertyType.CONFIGS, instanceName);

      nodeConfig.setSimpleField(InstanceConfigProperty.ENABLED.toString(),
          enable + "");
      accessor.setClusterProperty(ClusterPropertyType.CONFIGS, instanceName,
          nodeConfig);
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
        return;
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

  }

  public List<String> getNodeNamesInCluster(String clusterName)
  {
    String memberInstancesPath = CMUtil.getMemberInstancesPath(clusterName);
    return _zkClient.getChildren(memberInstancesPath);
  }

  @Override
  public void addResourceGroup(String clusterName, String dbName,
      int partitions, String stateModelRef)
  {
    ZNRecord idealState = new ZNRecord();
    idealState.setId(dbName);
    idealState.setSimpleField("partitions", String.valueOf(partitions));
    idealState.setSimpleField("state_model_def_ref", stateModelRef);

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

  public List<String> getResourceGroupsInCluster(String clusterName)
  {
    return _zkClient.getChildren(CMUtil.getIdealStatePath(clusterName));
  }

  @Override
  public ZNRecord getResourceGroupIdealState(String clusterName, String dbName)
  {
    return new ZKDataAccessor(clusterName, _zkClient).getClusterProperty(
        ClusterPropertyType.IDEALSTATES, dbName);
  }

  @Override
  public void setResourceGroupIdealState(String clusterName, String dbName,
      ZNRecord idealState)
  {
    new ZKDataAccessor(clusterName, _zkClient).setClusterProperty(
        ClusterPropertyType.IDEALSTATES, dbName, idealState);
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDef,
      ZNRecord record)
  {
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
    new ZKDataAccessor(clusterName, _zkClient).removeClusterProperty(
        ClusterPropertyType.IDEALSTATES, resourceGroup);
  }
  
  public List<String> getStateModelDefs(String clusterName)
  {
    return _zkClient.getChildren(CMUtil.getStateModelDefinitionPath(clusterName));
  }
}
