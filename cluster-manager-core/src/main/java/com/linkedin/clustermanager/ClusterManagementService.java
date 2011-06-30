package com.linkedin.clustermanager;

import java.util.List;


public interface ClusterManagementService 
{
  List<String> getClusters();

  List<String> getNodeNamesInCluster(String clusterName);

  List<String> getDatabasesInCluster(String clusterName);

  void addCluster(String clusterName, boolean overwritePrevRecord);

  void addDatabase(String clusterName, String dbName, int partitions);

  void addNode(String clusterName, ZNRecord nodeConfig);
    
  ZNRecord getDBIdealState(String clusterName, String dbName);
    
  void setDBIdealState(String clusterName, String dbName, ZNRecord idealState);
    
  void enableInstance(String clusterName, String instanceName, boolean enabled);
}
