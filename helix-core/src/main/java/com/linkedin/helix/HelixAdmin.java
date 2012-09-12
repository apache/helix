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
package com.linkedin.helix;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
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

  void addClusterToGrandCluster(String clusterName, String grandCluster);

  void addResource(String clusterName,
                   String resourceName,
                   int numResources,
                   String stateModelRef);

  void addResource(String clusterName,
                   String resourceName,
                   int numResources,
                   String stateModelRef,
                   String idealStateMode);

  void addResource(String clusterName,
                   String resourceName,
                   int numResources,
                   String stateModelRef,
                   String idealStateMode,
                   int bucketSize);

  void addInstance(String clusterName, InstanceConfig instanceConfig);

  void dropInstance(String clusterName, InstanceConfig instanceConfig);

  IdealState getResourceIdealState(String clusterName, String dbName);

  void setResourceIdealState(String clusterName,
                             String resourceName,
                             IdealState idealState);

  void enableInstance(String clusterName, String instanceName, boolean enabled);

  void enablePartition(boolean enabled,
                       String clusterName,
                       String instanceName,
                       String resourceName,
                       List<String> partitionNames);

  void enableCluster(String clusterName, boolean enabled);

  void resetPartition(String clusterName,
                      String instanceName,
                      String resourceName,
                      List<String> partitionNames);

  void resetInstance(String clusterName,
                      List<String> instanceNames);

  
  void addStateModelDef(String clusterName,
                        String stateModelDef,
                        StateModelDefinition record);

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

  void removeConfig(ConfigScope scope, Set<String> keys);

  Map<String, String> getConfig(ConfigScope scope, Set<String> keys);

  List<String> getConfigKeys(ConfigScopeProperty scope,
                             String clusterName,
                             String... keys);
}
