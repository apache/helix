package org.apache.helix.mock;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;

public class MockHelixAdmin implements HelixAdmin {

  private HelixDataAccessor _dataAccessor;
  private BaseDataAccessor _baseDataAccessor;

  public MockHelixAdmin(HelixManager manager) {
    _dataAccessor = manager.getHelixDataAccessor();
    _baseDataAccessor = _dataAccessor.getBaseDataAccessor();
  }

  @Override public List<String> getClusters() {
    return null;
  }

  @Override public List<String> getInstancesInCluster(String clusterName) {
    return null;
  }

  @Override public InstanceConfig getInstanceConfig(String clusterName, String instanceName) {
    return null;
  }

  @Override public boolean setInstanceConfig(String clusterName, String instanceName,
      InstanceConfig instanceConfig) {
    return false;
  }

  @Override public List<String> getResourcesInCluster(String clusterName) {
    return null;
  }

  @Override public List<String> getResourcesInClusterWithTag(String clusterName, String tag) {
    return null;
  }

  @Override public boolean addCluster(String clusterName) {
    return addCluster(clusterName, false);
  }

  @Override public boolean addCluster(String clusterName, boolean recreateIfExists) {
    String root = "/" + clusterName;
    _baseDataAccessor.exists(root, 0);
    createZKPaths(clusterName);
    return true;
  }

  private void createZKPaths(String clusterName) {
    String path;

    // IDEAL STATE
    _baseDataAccessor
        .create(PropertyPathBuilder.idealState(clusterName), new ZNRecord(clusterName), 0);
    // CONFIGURATIONS
    path = PropertyPathBuilder.clusterConfig(clusterName);
    _baseDataAccessor.create(path, new ClusterConfig(clusterName).getRecord(), 0);

    path = PropertyPathBuilder.instanceConfig(clusterName);
    _baseDataAccessor.create(path, new ZNRecord(clusterName), 0);

    path = PropertyPathBuilder.resourceConfig(clusterName);
    _baseDataAccessor.create(path, new ZNRecord(clusterName), 0);
    // PROPERTY STORE
    path = PropertyPathBuilder.propertyStore(clusterName);
    _baseDataAccessor.create(path, new ZNRecord(clusterName), 0);
    // LIVE INSTANCES
    _baseDataAccessor
        .create(PropertyPathBuilder.liveInstance(clusterName), new ZNRecord(clusterName), 0);
    // MEMBER INSTANCES
    _baseDataAccessor
        .create(PropertyPathBuilder.instance(clusterName), new ZNRecord(clusterName), 0);
    // External view
    _baseDataAccessor
        .create(PropertyPathBuilder.externalView(clusterName), new ZNRecord(clusterName), 0);
    // State model definition
    _baseDataAccessor
        .create(PropertyPathBuilder.stateModelDef(clusterName), new ZNRecord(clusterName), 0);

    // controller
    _baseDataAccessor
        .create(PropertyPathBuilder.controller(clusterName), new ZNRecord(clusterName), 0);
    path = PropertyPathBuilder.controllerHistory(clusterName);
    final ZNRecord emptyHistory = new ZNRecord(PropertyType.HISTORY.toString());
    final List<String> emptyList = new ArrayList<String>();
    emptyHistory.setListField(clusterName, emptyList);
    _baseDataAccessor.create(path, emptyHistory, 0);

    path = PropertyPathBuilder.controllerMessage(clusterName);
    _baseDataAccessor.create(path, new ZNRecord(clusterName), 0);

    path = PropertyPathBuilder.controllerStatusUpdate(clusterName);
    _baseDataAccessor.create(path, new ZNRecord(clusterName), 0);

    path = PropertyPathBuilder.controllerError(clusterName);
    _baseDataAccessor.create(path, new ZNRecord(clusterName), 0);
  }

  @Override public void addClusterToGrandCluster(String clusterName, String grandCluster) {

  }

  @Override public void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef) {

  }

  @Override public void addResource(String clusterName, String resourceName,
      IdealState idealstate) {

  }

  @Override public void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef, String rebalancerMode) {

  }

  @Override public void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef, String rebalancerMode, String rebalanceStrategy) {

  }

  @Override public void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef, String rebalancerMode, int bucketSize) {

  }

  @Override public void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef, String rebalancerMode, int bucketSize, int maxPartitionsPerInstance) {

  }

  @Override public void addResource(String clusterName, String resourceName, int numPartitions,
      String stateModelRef, String rebalancerMode, String rebalanceStrategy, int bucketSize,
      int maxPartitionsPerInstance) {

  }

  @Override public void addInstance(String clusterName, InstanceConfig instanceConfig) {
    String instanceConfigsPath = PropertyPathBuilder.instanceConfig(clusterName);
    String nodeId = instanceConfig.getId();
    if (!_baseDataAccessor.exists(instanceConfigsPath, 0)) {
      _baseDataAccessor.create(instanceConfigsPath, new ZNRecord(nodeId), 0);
    }

    String instanceConfigPath = instanceConfigsPath + "/" + nodeId;

    _baseDataAccessor.create(instanceConfigPath, instanceConfig.getRecord(), 0);
    _baseDataAccessor
        .set(PropertyPathBuilder.instanceMessage(clusterName, nodeId), new ZNRecord(nodeId), 0);
    _baseDataAccessor
        .set(PropertyPathBuilder.instanceCurrentState(clusterName, nodeId), new ZNRecord(nodeId),
            0);
    _baseDataAccessor
        .set(PropertyPathBuilder.instanceError(clusterName, nodeId), new ZNRecord(nodeId), 0);
    _baseDataAccessor
        .set(PropertyPathBuilder.instanceStatusUpdate(clusterName, nodeId), new ZNRecord(nodeId),
            0);
    _baseDataAccessor
        .set(PropertyPathBuilder.instanceHistory(clusterName, nodeId), new ZNRecord(nodeId), 0);
  }

  @Override public void dropInstance(String clusterName, InstanceConfig instanceConfig) {

  }

  @Override public IdealState getResourceIdealState(String clusterName, String resourceName) {
    return null;
  }

  @Override public void setResourceIdealState(String clusterName, String resourceName,
      IdealState idealState) {

  }

  @Override public void enableInstance(String clusterName, String instanceName, boolean enabled) {

  }

  @Override public void enableResource(String clusterName, String resourceName, boolean enabled) {

  }

  @Override public void enablePartition(boolean enabled, String clusterName, String instanceName,
      String resourceName, List<String> partitionNames) {

  }

  @Override public void enableCluster(String clusterName, boolean enabled) {

  }

  @Override public void enableCluster(String clusterName, boolean enabled, String reason) {

  }

  @Override public void resetPartition(String clusterName, String instanceName, String resourceName,
      List<String> partitionNames) {

  }

  @Override public void resetInstance(String clusterName, List<String> instanceNames) {

  }

  @Override public void resetResource(String clusterName, List<String> resourceNames) {

  }

  @Override public void addStateModelDef(String clusterName, String stateModelDef,
      StateModelDefinition record) {

  }

  @Override public void addStateModelDef(String clusterName, String stateModelDef,
      StateModelDefinition record, boolean recreateIfExists) {

  }

  @Override public void dropResource(String clusterName, String resourceName) {

  }

  @Override public List<String> getStateModelDefs(String clusterName) {
    return null;
  }

  @Override public StateModelDefinition getStateModelDef(String clusterName,
      String stateModelName) {
    return null;
  }

  @Override public ExternalView getResourceExternalView(String clusterName, String resourceName) {
    return null;
  }

  @Override public void dropCluster(String clusterName) {

  }

  @Override public void setConfig(HelixConfigScope scope, Map<String, String> properties) {

  }

  @Override public void removeConfig(HelixConfigScope scope, List<String> keys) {

  }

  @Override public Map<String, String> getConfig(HelixConfigScope scope, List<String> keys) {
    return null;
  }

  @Override public List<String> getConfigKeys(HelixConfigScope scope) {
    return null;
  }

  @Override public void rebalance(String clusterName, String resourceName, int replica) {

  }

  @Override public void addIdealState(String clusterName, String resourceName,
      String idealStateFile) throws IOException {

  }

  @Override public void addStateModelDef(String clusterName, String stateModelDefName,
      String stateModelDefFile) throws IOException {

  }

  @Override public void setConstraint(String clusterName,
      ClusterConstraints.ConstraintType constraintType, String constraintId,
      ConstraintItem constraintItem) {

  }

  @Override public void removeConstraint(String clusterName,
      ClusterConstraints.ConstraintType constraintType, String constraintId) {

  }

  @Override public ClusterConstraints getConstraints(String clusterName,
      ClusterConstraints.ConstraintType constraintType) {
    return null;
  }

  @Override public void rebalance(String clusterName, IdealState currentIdealState,
      List<String> instanceNames) {

  }

  @Override public void rebalance(String clusterName, String resourceName, int replica,
      List<String> instances) {

  }

  @Override public void rebalance(String clusterName, String resourceName, int replica,
      String keyPrefix, String group) {

  }

  @Override public List<String> getInstancesInClusterWithTag(String clusterName, String tag) {
    return null;
  }

  @Override public void addInstanceTag(String clusterName, String instanceName, String tag) {

  }

  @Override public void removeInstanceTag(String clusterName, String instanceName, String tag) {

  }

  @Override public void setInstanceZoneId(String clusterName, String instanceName, String zoneId) {

  }

  @Override public void enableBatchMessageMode(String clusterName, boolean enabled) {

  }

  @Override public void enableBatchMessageMode(String clusterName, String resourceName,
      boolean enabled) {

  }

  @Override public void close() {

  }
}
