package com.linkedin.helix.agent.file;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterManagementService;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.store.PropertyStoreException;
import com.linkedin.helix.store.file.FilePropertyStore;
import com.linkedin.helix.tools.StateModelConfigGenerator;
import com.linkedin.helix.util.CMUtil;

public class FileClusterManagementTool implements ClusterManagementService
{
  private static Logger logger = Logger.getLogger(FileClusterManagementTool.class);
  private final FilePropertyStore<ZNRecord> _store;

  public FileClusterManagementTool(FilePropertyStore<ZNRecord> store)
  {
    _store = store;
  }

  @Override
  public List<String> getClusters()
  {
    throw new UnsupportedOperationException(
      "getClusters() is NOT supported by FileClusterManagementTool");

  }

  @Override
  public List<String> getInstancesInCluster(String clusterName)
  {
    String path = CMUtil.getConfigPath(clusterName);

    List<String> childs = null;
    List<String> instanceNames = new ArrayList<String>();
    try
    {
      childs = _store.getPropertyNames(path);
      for (String child : childs)
      {
        // strip config path from instanceName
        String instanceName = child.substring(child.lastIndexOf('/') + 1);
        instanceNames.add(instanceName);
      }
      return instanceNames;
    }
    catch (PropertyStoreException e)
    {
      logger.error("Fail to getInstancesInCluster, cluster " + clusterName, e);
    }

    return null;
  }

  @Override
  public List<String> getResourceGroupsInCluster(String clusterName)
  {
    // TODO Auto-generated method stub
    // return null;
    throw new UnsupportedOperationException(
      "getResourceGroupsInCluster() is NOT supported by FileClusterManagementTool");

  }

  @Override
  public void addCluster(String clusterName, boolean overwritePrevRecord)
  {
    try
    {
//      if (_store.getProperty(path) != null)
//      {
//        LOG.warn("Target directory exists.Cleaning the target directory:" + path
//            + " overwritePrevRecord: " + overwritePrevRecord);
//        if (overwritePrevRecord)
//        {
//          _store.removeProperty(path);
//        } else
//        {
//          throw new PropertyStoreException("Target directory already exists, " +
//              "overwritePrevRecord: " + overwritePrevRecord);
//        }
//      }

      _store.removeNamespace(clusterName);
      _store.createPropertyNamespace(clusterName);

      _store.createPropertyNamespace(CMUtil.getIdealStatePath(clusterName));
      _store.createPropertyNamespace(CMUtil.getConfigPath(clusterName));
      _store.createPropertyNamespace(CMUtil.getLiveInstancesPath(clusterName));
      _store.createPropertyNamespace(CMUtil.getMemberInstancesPath(clusterName));
      _store.createPropertyNamespace(CMUtil.getExternalViewPath(clusterName));
      _store.createPropertyNamespace(CMUtil.getStateModelDefinitionPath(clusterName));

      StateModelConfigGenerator generator = new StateModelConfigGenerator();
      addStateModelDef(clusterName,
                       "MasterSlave",
                       new StateModelDefinition(generator.generateConfigForMasterSlave()));

    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to add cluster " + clusterName + "\nexception: " + e);
    }

  }



  @Override
  public void addResourceGroup(String clusterName, String resourceGroup, int numResources,
                               String stateModelRef)
  {
    String idealStatePath = CMUtil.getIdealStatePath(clusterName);
    String resourceGroupIdealStatePath = idealStatePath + "/" + resourceGroup;

//    if (_zkClient.exists(dbIdealStatePath))
//    {
//      logger.warn("Skip the operation. DB ideal state directory exists:"
//          + dbIdealStatePath);
//      return;
//    }

    IdealState idealState = new IdealState(resourceGroup);
    idealState.setNumPartitions(numResources);
    idealState.setStateModelDefRef(stateModelRef);
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
    try
    {
      _store.setProperty(resourceGroupIdealStatePath, idealState.getRecord());
    }
    catch (PropertyStoreException e)
    {
      logger.error("Fail to add resource group, cluster:" + clusterName +
          " resource group:" + resourceGroup +
          "\nexception: " + e);
    }

  }

  @Override
  public void addResourceGroup(String clusterName, String resourceGroup, int numResources,
                               String stateModelRef, String idealStateMode)
  {
    throw new UnsupportedOperationException("ideal state mode not supported in file-based cluster manager");
  }

  @Override
  public void addInstance(String clusterName, InstanceConfig config)
  {
    String configsPath = CMUtil.getConfigPath(clusterName);
    String nodeId = config.getId();
    String nodeConfigPath = configsPath + "/" + nodeId;

    try
    {
      _store.setProperty(nodeConfigPath, config.getRecord());
      _store.createPropertyNamespace(CMUtil.getMessagePath(clusterName, nodeId));
      _store.createPropertyNamespace(CMUtil.getCurrentStateBasePath(clusterName, nodeId));
      _store.createPropertyNamespace(CMUtil.getErrorsPath(clusterName, nodeId));
      _store.createPropertyNamespace(CMUtil.getStatusUpdatesPath(clusterName, nodeId));
    }
    catch(Exception e)
    {
      logger.error("Fail to add node, cluster:" + clusterName +
          "\nexception: " + e);
    }

  }

  @Override
  public void dropInstance(String clusterName, InstanceConfig config) {
	  String configsPath = CMUtil.getConfigPath(clusterName);
	  String nodeId = config.getId();
	  String nodeConfigPath = configsPath + "/" + nodeId;

	  try
    {
      _store.setProperty(nodeConfigPath, config.getRecord());
    }
    catch(Exception e)
    {
      logger.error("Fail to drop node, cluster:" + clusterName, e);
    }
  }

  @Override
  public IdealState getResourceGroupIdealState(String clusterName, String resourceGroupName)
  {
    return new FileBasedDataAccessor(_store, clusterName).getProperty(IdealState.class,
                                                                      PropertyType.IDEALSTATES,
                                                                      resourceGroupName);
  }

  @Override
  public void setResourceGroupIdealState(String clusterName, String resourceGroupName,
                                         IdealState idealState)
  {
    new FileBasedDataAccessor(_store, clusterName).setProperty(PropertyType.IDEALSTATES,
                                                               idealState,
                                                               resourceGroupName);
  }

  @Override
  public void enableInstance(String clusterName, String instanceName, boolean enabled)
  {
    throw new UnsupportedOperationException(
      "enableInstance() is NOT supported by FileClusterManagementTool");
  }

  @Override
  public void addStateModelDef(String clusterName, String stateModelDef,
                               StateModelDefinition stateModel)
  {

    String stateModelDefPath = CMUtil.getStateModelDefinitionPath(clusterName);
    String stateModelPath = stateModelDefPath + "/" + stateModelDef;

    try
    {
      _store.setProperty(stateModelPath, stateModel.getRecord());
    }
    catch (PropertyStoreException e)
    {
      logger.error("Fail to addStateModelDef, cluster:" + clusterName +
          " stateModelDef:" + stateModelDef, e);
    }

  }

  @Override
  public void dropResourceGroup(String clusterName, String resourceGroup)
  {
    new FileBasedDataAccessor(_store, clusterName).removeProperty(
        PropertyType.IDEALSTATES, resourceGroup);

  }

  @Override
  public List<String> getStateModelDefs(String clusterName)
  {
    throw new UnsupportedOperationException(
      "getStateModelDefs() is NOT supported by FileClusterManagementTool");
  }

  @Override
  public InstanceConfig getInstanceConfig(String clusterName, String instanceName)
  {
    throw new UnsupportedOperationException(
        "getInstanceConfig() is NOT supported by FileClusterManagementTool");
  }

  @Override
  public StateModelDefinition getStateModelDef(String clusterName, String stateModelName)
  {
    throw new UnsupportedOperationException(
      "getStateModelDef() is NOT supported by FileClusterManagementTool");
  }

  @Override
  public ExternalView getResourceGroupExternalView(String clusterName, String resourceGroup)
  {
    throw new UnsupportedOperationException(
        "getResourceGroupExternalView() is NOT supported by FileClusterManagementTool");
  }

  @Override
  public void enablePartition(String clusterName,
                              String instanceName,
                              String partition,
                              boolean enabled)
  {
    throw new UnsupportedOperationException(
        "enablePartition() is NOT supported by FileClusterManagementTool");
  }

  @Override
  public void resetPartition(String clusterName,
		  String instanceName,
		  String resourceGroupName,
		  String partition)
  {
	  // TODO Auto-generated method stub
	  throw new UnsupportedOperationException(
			  "resetPartition() is NOT supported by FileClusterManagementTool");
  }

  @Override
  public void addStat(String clusterName, String statName) {
	  throw new UnsupportedOperationException(
			  "addStat() is NOT supported by FileClusterManagementTool");

  }

  @Override
  public void addAlert(String clusterName, String alertName) {
	  throw new UnsupportedOperationException(
			  "addAlert() is NOT supported by FileClusterManagementTool");

  }
  
  @Override
  public void dropStat(String clusterName, String statName) {
	  throw new UnsupportedOperationException(
			  "dropStat() is NOT supported by FileClusterManagementTool");

  }

  @Override
  public void dropAlert(String clusterName, String alertName) {
	  throw new UnsupportedOperationException(
			  "dropAlert() is NOT supported by FileClusterManagementTool");

  }

  @Override
  public void dropCluster(String clusterName)
  {
    // TODO Auto-generated method stub
    
  }

}
