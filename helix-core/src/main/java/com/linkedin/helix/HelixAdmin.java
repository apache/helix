package com.linkedin.helix;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.StateModelDefinition;

public interface HelixAdmin
{
  List<String> getClusters();

  List<String> getInstancesInCluster(String clusterName);

  InstanceConfig getInstanceConfig(String clusterName, String instanceName);

  List<String> getResourcesInCluster(String clusterName);

  void addCluster(String clusterName, boolean overwritePrevRecord);
  
  void addCluster(String clusterName, boolean overwritePrevRecord,
      String grandCluster);

  void addResource(String clusterName, String resourceName, int numResources,
                        String stateModelRef);

  void addResource(String clusterName, String resourceName, int numResources,
                        String stateModelRef, String idealStateMode);

  void addInstance(String clusterName, InstanceConfig instanceConfig);

  void dropInstance(String clusterName, InstanceConfig instanceConfig);

  IdealState getResourceIdealState(String clusterName, String dbName);

  void setResourceIdealState(String clusterName, String resourceName, IdealState idealState);

  void enableInstance(String clusterName, String instanceName, boolean enabled);

  void enablePartition(String clusterName,
                       String instanceName,
                       String resourceName,
                       String partition,
                       boolean enabled);

  void resetPartition(String clusterName, String instanceName, String resourceName, String partition);

  void addStateModelDef(String clusterName, String stateModelDef, StateModelDefinition record);

  void dropResource(String clusterName, String resourceName);

  void addStat(String clusterName, String statName);

  void addAlert(String clusterName, String alertName);

  void dropStat(String clusterName, String statName);

  void dropAlert(String clusterName, String alertName);

  List<String> getStateModelDefs(String clusterName);

  StateModelDefinition getStateModelDef(String clusterName, String stateModelName);

  ExternalView getResourceExternalView(String clusterName, String resourceName);

  void dropCluster(String clusterName);
  
  void setConfig(ConfigScope scope, Map<String, String> properties);

  Map<String, String> getConfig(ConfigScope scope, Set<String> keys);
}
