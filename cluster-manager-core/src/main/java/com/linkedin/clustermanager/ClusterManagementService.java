package com.linkedin.clustermanager;

import java.util.List;

public interface ClusterManagementService
{
  List<String> getClusters();

  List<String> getNodeNamesInCluster(String clusterName);

  List<String> getResourceGroupsInCluster(String clusterName);

  void addCluster(String clusterName, boolean overwritePrevRecord);

  void addResourceGroup(String clusterName, String resourceGroup, int numResources, String stateModelRef);

  void addNode(String clusterName, ZNRecord nodeConfig);

  ZNRecord getResourceGroupIdealState(String clusterName, String dbName);

  void setResourceGroupIdealState(String clusterName, String resourceGroup, ZNRecord idealState);

  void enableInstance(String clusterName, String instanceName, boolean enabled);
  
  void addStateModelDef(String clusterName, String stateModelDef, ZNRecord record);

  void dropResourceGroup(String clusterName, String resourceGroup);
  
  List<String> getStateModelDefs(String clusterName);
}
