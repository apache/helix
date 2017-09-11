package org.apache.helix.integration.common;

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

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class ZkIntegrationTestBase {
  private static Logger LOG = Logger.getLogger(ZkIntegrationTestBase.class);

  protected static ZkServer _zkServer;
  protected static ZkClient _gZkClient;
  protected static ClusterSetup _gSetupTool;
  protected static BaseDataAccessor<ZNRecord> _baseAccessor;

  public static final String ZK_ADDR = "localhost:2183";
  protected static final String CLUSTER_PREFIX = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  protected final String CONTROLLER_PREFIX = "controller";
  protected final String PARTICIPANT_PREFIX = "localhost";

  @BeforeSuite
  public void beforeSuite() throws Exception {
    // TODO: use logging.properties file to config java.util.logging.Logger levels
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);

    _zkServer = TestHelper.startZkServer(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    ZKClientPool.reset();

    _gZkClient = new ZkClient(ZK_ADDR);
    _gZkClient.setZkSerializer(new ZNRecordSerializer());
    _gSetupTool = new ClusterSetup(_gZkClient);
    _baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);
  }

  @AfterSuite
  public void afterSuite() {
    ZKClientPool.reset();
    _gZkClient.close();
    TestHelper.stopZkServer(_zkServer);
  }

  protected String getShortClassName() {
    return this.getClass().getSimpleName();
  }

  protected String getCurrentLeader(ZkClient zkClient, String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader == null) {
      return null;
    }
    return leader.getInstanceName();
  }

  protected void enableHealthCheck(String clusterName) {
    ConfigScope scope = new ConfigScopeBuilder().forCluster(clusterName).build();
    new ConfigAccessor(_gZkClient).set(scope, "healthChange" + ".enabled", "" + true);
  }

  protected void enablePersistBestPossibleAssignment(ZkClient zkClient, String clusterName,
      Boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setPersistBestPossibleAssignment(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected void enableTopologyAwareRebalance(ZkClient zkClient, String clusterName,
      Boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setTopologyAwareEnabled(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected void enableDelayRebalanceInCluster(ZkClient zkClient, String clusterName,
      boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setDelayRebalaceEnabled(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected void enableDelayRebalanceInInstance(ZkClient zkClient, String clusterName,
      String instanceName, boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    instanceConfig.setDelayRebalanceEnabled(enabled);
    configAccessor.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  protected void setDelayTimeInCluster(ZkClient zkClient, String clusterName, long delay) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setRebalanceDelayTime(delay);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected IdealState createResourceWithDelayedRebalance(String clusterName, String db,
      String stateModel, int numPartition, int replica, int minActiveReplica, long delay) {
    return createResourceWithDelayedRebalance(clusterName, db, stateModel, numPartition, replica,
        minActiveReplica, delay, AutoRebalanceStrategy.class.getName());
  }

  protected IdealState createResourceWithDelayedRebalance(String clusterName, String db,
      String stateModel, int numPartition, int replica, int minActiveReplica, long delay,
      String rebalanceStrategy) {
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(clusterName, db);
    if (idealState == null) {
      _gSetupTool.addResourceToCluster(clusterName, db, numPartition, stateModel,
          IdealState.RebalanceMode.FULL_AUTO + "", rebalanceStrategy);
    }

    idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(clusterName, db);
    idealState.setMinActiveReplicas(minActiveReplica);
    if (!idealState.isDelayRebalanceEnabled()) {
      idealState.setDelayRebalanceEnabled(true);
    }
    if (delay > 0) {
      idealState.setRebalanceDelay(delay);
    }
    idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(clusterName, db, idealState);
    _gSetupTool.rebalanceStorageCluster(clusterName, db, replica);
    idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(clusterName, db);

    return idealState;
  }

  /**
   * Validate there should be always minimal active replica and top state replica for each partition.
   * Also make sure there is always some partitions with only active replica count.
   */
  protected void validateMinActiveAndTopStateReplica(IdealState is, ExternalView ev,
      int minActiveReplica, int numNodes) {
    StateModelDefinition stateModelDef =
        BuiltInStateModelDefinitions.valueOf(is.getStateModelDefRef()).getStateModelDefinition();
    String topState = stateModelDef.getStatesPriorityList().get(0);
    int replica = Integer.valueOf(is.getReplicas());

    Map<String, Integer> stateCount =
        stateModelDef.getStateCountMap(numNodes, replica);
    Set<String> activeStates = stateCount.keySet();

    for (String partition : is.getPartitionSet()) {
      Map<String, String> assignmentMap = ev.getRecord().getMapField(partition);
      Assert.assertNotNull(assignmentMap,
          is.getResourceName() + "'s best possible assignment is null for partition " + partition);
      Assert.assertTrue(!assignmentMap.isEmpty(),
          is.getResourceName() + "'s partition " + partition + " has no best possible map in IS.");

      boolean hasTopState = false;
      int activeReplica = 0;
      for (String state : assignmentMap.values()) {
        if (topState.equalsIgnoreCase(state)) {
          hasTopState = true;
        }
        if (activeStates.contains(state)) {
          activeReplica++;
        }
      }

      Assert.assertTrue(hasTopState, String.format("%s missing %s replica", partition, topState));
      Assert.assertTrue(activeReplica >= minActiveReplica, String
          .format("%s has less active replica %d then required %d", partition, activeReplica,
              minActiveReplica));
    }
  }
}
