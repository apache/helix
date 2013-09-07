package org.apache.helix.api;

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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.controller.rebalancer.NewAutoRebalancer;
import org.apache.helix.controller.rebalancer.NewCustomRebalancer;
import org.apache.helix.controller.rebalancer.NewSemiAutoRebalancer;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.NewBestPossibleStateCalcStage;
import org.apache.helix.controller.stages.NewBestPossibleStateOutput;
import org.apache.helix.controller.stages.NewCurrentStateOutput;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class TestNewStages extends ZkUnitTestBase {
  final int n = 2;
  final int p = 8;
  final int r = 2;
  MockParticipant[] _participants = new MockParticipant[n];
  ClusterController _controller;

  ClusterId _clusterId;
  HelixDataAccessor _dataAccessor;

  @Test
  public void testReadClusterDataStage() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    ClusterAccessor clusterAccessor = new ClusterAccessor(_clusterId, _dataAccessor);
    Cluster cluster = clusterAccessor.readCluster();

    ClusterId id = cluster.getId();
    Assert.assertEquals(id, _clusterId);
    Map<ParticipantId, Participant> liveParticipantMap = cluster.getLiveParticipantMap();
    Assert.assertEquals(liveParticipantMap.size(), n);

    for (ParticipantId participantId : liveParticipantMap.keySet()) {
      Participant participant = liveParticipantMap.get(participantId);
      Map<ResourceId, CurrentState> curStateMap = participant.getCurrentStateMap();
      Assert.assertEquals(curStateMap.size(), 1);

      ResourceId resourceId = new ResourceId("TestDB0");
      Assert.assertTrue(curStateMap.containsKey(resourceId));
      CurrentState curState = curStateMap.get(resourceId);
      Map<PartitionId, State> partitionStateMap = curState.getPartitionStateMap();
      Assert.assertEquals(partitionStateMap.size(), p);
    }

    Map<ResourceId, Resource> resourceMap = cluster.getResourceMap();
    Assert.assertEquals(resourceMap.size(), 1);

    ResourceId resourceId = new ResourceId("TestDB0");
    Assert.assertTrue(resourceMap.containsKey(resourceId));
    Resource resource = resourceMap.get(resourceId);
    Assert
        .assertEquals(resource.getRebalancerConfig().getRebalancerMode(), RebalanceMode.SEMI_AUTO);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testBasicBestPossibleStateCalcStage() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    // Set up the event
    ClusterAccessor clusterAccessor = new ClusterAccessor(_clusterId, _dataAccessor);
    Cluster cluster = clusterAccessor.readCluster();
    ClusterEvent event = new ClusterEvent(testName);
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), new NewCurrentStateOutput());
    Map<ResourceId, ResourceConfig> resourceConfigMap =
        Maps.transformValues(cluster.getResourceMap(), new Function<Resource, ResourceConfig>() {
          @Override
          public ResourceConfig apply(Resource resource) {
            return resource.getConfig();
          }
        });
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceConfigMap);
    event.addAttribute("ClusterDataCache", cluster);
    Map<StateModelDefId, StateModelDefinition> stateModelMap =
        new HashMap<StateModelDefId, StateModelDefinition>();
    stateModelMap.put(Id.stateModelDef("MasterSlave"), new StateModelDefinition(
        StateModelConfigGenerator.generateConfigForMasterSlave()));
    event.addAttribute(AttributeName.STATE_MODEL_DEFINITIONS.toString(), stateModelMap);

    // Run the stage
    try {
      new NewBestPossibleStateCalcStage().process(event);
    } catch (Exception e) {
      Assert.fail(e.toString());
    }

    // Verify the result
    NewBestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    Assert.assertNotNull(bestPossibleStateOutput);
    ResourceId resourceId = new ResourceId("TestDB0");
    ResourceAssignment assignment = bestPossibleStateOutput.getResourceAssignment(resourceId);
    Assert.assertNotNull(assignment);
    Resource resource = cluster.getResource(resourceId);
    verifySemiAutoRebalance(resource, assignment);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testClusterRebalancers() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    ClusterAccessor clusterAccessor = new ClusterAccessor(_clusterId, _dataAccessor);
    Cluster cluster = clusterAccessor.readCluster();

    ResourceId resourceId = new ResourceId("TestDB0");
    Resource resource = cluster.getResource(resourceId);
    StateModelDefinition masterSlave =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    NewCurrentStateOutput currentStateOutput = new NewCurrentStateOutput();
    ResourceAssignment fullAutoResult =
        new NewAutoRebalancer().computeResourceMapping(resource, cluster, masterSlave,
            currentStateOutput);
    verifyFullAutoRebalance(resource, fullAutoResult);
    ResourceAssignment semiAutoResult =
        new NewSemiAutoRebalancer().computeResourceMapping(resource, cluster, masterSlave,
            currentStateOutput);
    verifySemiAutoRebalance(resource, semiAutoResult);
    ResourceAssignment customResult =
        new NewCustomRebalancer().computeResourceMapping(resource, cluster, masterSlave,
            currentStateOutput);
    verifyCustomRebalance(resource, customResult);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Check that a full auto rebalance is run, and at least one replica per partition is mapped
   * @param resource the resource to verify
   * @param assignment the assignment to verify
   */
  private void verifyFullAutoRebalance(Resource resource, ResourceAssignment assignment) {
    Assert.assertEquals(assignment.getMappedPartitions().size(), resource.getPartitionSet().size());
    for (PartitionId partitionId : assignment.getMappedPartitions()) {
      Map<ParticipantId, State> replicaMap = assignment.getReplicaMap(partitionId);
      Assert.assertTrue(replicaMap.size() <= r);
      Assert.assertTrue(replicaMap.size() > 0);
      boolean hasMaster = false;
      for (State state : replicaMap.values()) {
        if (state.equals(State.from("MASTER"))) {
          Assert.assertFalse(hasMaster);
          hasMaster = true;
        }
      }
      Assert.assertTrue(hasMaster);
    }
  }

  /**
   * Check that a semi auto rebalance is run, and all partitions are mapped by preference list
   * @param resource the resource to verify
   * @param assignment the assignment to verify
   */
  private void verifySemiAutoRebalance(Resource resource, ResourceAssignment assignment) {
    Assert.assertEquals(assignment.getMappedPartitions().size(), resource.getPartitionSet().size());
    RebalancerConfig config = resource.getRebalancerConfig();
    for (PartitionId partitionId : assignment.getMappedPartitions()) {
      List<ParticipantId> preferenceList = config.getPreferenceList(partitionId);
      Map<ParticipantId, State> replicaMap = assignment.getReplicaMap(partitionId);
      Assert.assertEquals(replicaMap.size(), preferenceList.size());
      Assert.assertEquals(replicaMap.size(), r);
      boolean hasMaster = false;
      for (ParticipantId participant : preferenceList) {
        Assert.assertTrue(replicaMap.containsKey(participant));
        State state = replicaMap.get(participant);
        if (state.equals(State.from("MASTER"))) {
          Assert.assertFalse(hasMaster);
          hasMaster = true;
        }
      }
      Assert.assertEquals(replicaMap.get(preferenceList.get(0)), State.from("MASTER"));
    }
  }

  /**
   * For vanilla customized rebalancing, the resource assignment should match the preference map
   * @param resource the resource to verify
   * @param assignment the assignment to verify
   */
  private void verifyCustomRebalance(Resource resource, ResourceAssignment assignment) {
    Assert.assertEquals(assignment.getMappedPartitions().size(), resource.getPartitionSet().size());
    RebalancerConfig config = resource.getRebalancerConfig();
    for (PartitionId partitionId : assignment.getMappedPartitions()) {
      Map<ParticipantId, State> preferenceMap = config.getPreferenceMap(partitionId);
      Map<ParticipantId, State> replicaMap = assignment.getReplicaMap(partitionId);
      Assert.assertEquals(replicaMap.size(), preferenceMap.size());
      Assert.assertEquals(replicaMap.size(), r);
      for (ParticipantId participant : preferenceMap.keySet()) {
        Assert.assertTrue(replicaMap.containsKey(participant));
        Assert.assertEquals(replicaMap.get(participant), preferenceMap.get(participant));
      }
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    // set up a running class
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    _clusterId = new ClusterId(clusterName);

    System.out.println("START " + _clusterId + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        p, // partitions per resource
        n, // number of nodes
        r, // replicas
        "MasterSlave", true); // do rebalance

    _controller = new ClusterController(clusterName, "controller_0", ZK_ADDR);
    _controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      _participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      _participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    _dataAccessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
  }

  @AfterClass
  public void afterClass() {
    // tear down the cluster
    _controller.syncStop();
    for (int i = 0; i < n; i++) {
      _participants[i].syncStop();
    }

    System.out.println("END " + _clusterId + " at " + new Date(System.currentTimeMillis()));
  }
}
