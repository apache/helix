package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.StateModelDefinition;

public interface ClusterManagementService
{
  List<String> getClusters();

  List<String> getInstancesInCluster(String clusterName);

  InstanceConfig getInstanceConfig(String clusterName, String instanceName);

  List<String> getResourceGroupsInCluster(String clusterName);

  void addCluster(String clusterName, boolean overwritePrevRecord);

  void addResourceGroup(String clusterName, String resourceGroup, int numResources,
                        String stateModelRef);

  void addResourceGroup(String clusterName, String resourceGroup, int numResources,
                        String stateModelRef, String idealStateMode);

  void addInstance(String clusterName, InstanceConfig instanceConfig);

  void dropInstance(String clusterName, InstanceConfig instanceConfig);

  IdealState getResourceGroupIdealState(String clusterName, String dbName);

  void setResourceGroupIdealState(String clusterName, String resourceGroup, IdealState idealState);

  void enableInstance(String clusterName, String instanceName, boolean enabled);

  void enablePartition(String clusterName, String instanceName, String partition,
                       boolean enabled);

  void resetPartition(String clusterName, String instanceName, String resourceGroupName, String partition);

  void addStateModelDef(String clusterName, String stateModelDef, StateModelDefinition record);

  void dropResourceGroup(String clusterName, String resourceGroup);

  void addStat(String clusterName, String statName);
  
  void addAlert(String clusterName, String alertName);
  
  List<String> getStateModelDefs(String clusterName);

  StateModelDefinition getStateModelDef(String clusterName, String stateModelName);

  ExternalView getResourceGroupExternalView(String clusterName, String resourceGroup);

  void dropCluster(String clusterName);
}
