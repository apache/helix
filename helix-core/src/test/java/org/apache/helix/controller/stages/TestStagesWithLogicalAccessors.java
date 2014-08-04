package org.apache.helix.controller.stages;

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
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Resource;
import org.apache.helix.api.State;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class TestStagesWithLogicalAccessors extends ZkTestBase {
  final int n = 2;
  final int p = 8;
  final int r = 2;
  MockParticipant[] _participants = new MockParticipant[n];
  MockController _controller;

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

      ResourceId resourceId = ResourceId.from("TestDB0");
      Assert.assertTrue(curStateMap.containsKey(resourceId));
      CurrentState curState = curStateMap.get(resourceId);
      Map<PartitionId, State> partitionStateMap = curState.getTypedPartitionStateMap();
      Assert.assertEquals(partitionStateMap.size(), p);
    }

    Map<ResourceId, Resource> resourceMap = cluster.getResourceMap();
    Assert.assertEquals(resourceMap.size(), 1);

    ResourceId resourceId = ResourceId.from("TestDB0");
    Assert.assertTrue(resourceMap.containsKey(resourceId));
    Resource resource = resourceMap.get(resourceId);
    Assert.assertNotNull(resource.getIdealState());
    Assert.assertEquals(resource.getIdealState().getRebalanceMode(), RebalanceMode.SEMI_AUTO);

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
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), new ResourceCurrentState());
    Map<ResourceId, ResourceConfig> resourceConfigMap =
        Maps.transformValues(cluster.getResourceMap(), new Function<Resource, ResourceConfig>() {
          @Override
          public ResourceConfig apply(Resource resource) {
            return resource.getConfig();
          }
        });
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceConfigMap);
    event.addAttribute("Cluster", cluster);

    // Run the stage
    try {
      new BestPossibleStateCalcStage().process(event);
    } catch (Exception e) {
      Assert.fail(e.toString());
    }

    // Verify the result
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    Assert.assertNotNull(bestPossibleStateOutput);
    ResourceId resourceId = ResourceId.from("TestDB0");
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

    ResourceId resourceId = ResourceId.from("TestDB0");
    Resource resource = cluster.getResource(resourceId);
    ResourceCurrentState currentStateOutput = new ResourceCurrentState();
    ResourceAssignment semiAutoResult =
        resource
            .getIdealState()
            .getRebalancerRef()
            .getRebalancer()
            .computeResourceMapping(resource.getIdealState(), resource.getRebalancerConfig(), null,
                cluster, currentStateOutput);
    verifySemiAutoRebalance(resource, semiAutoResult);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Check that a semi auto rebalance is run, and all partitions are mapped by preference list
   * @param resource the resource to verify
   * @param assignment the assignment to verify
   */
  private void verifySemiAutoRebalance(Resource resource, ResourceAssignment assignment) {
    Assert.assertEquals(assignment.getMappedPartitionIds().size(), resource.getSubUnitSet().size());
    IdealState idealState = resource.getIdealState();
    for (PartitionId partitionId : assignment.getMappedPartitionIds()) {
      List<ParticipantId> preferenceList = idealState.getPreferenceList(partitionId);
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

  @BeforeClass
  public void beforeClass() throws Exception {
    // set up a running class
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    _clusterId = ClusterId.from(clusterName);

    System.out.println("START " + _clusterId + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        p, // partitions per resource
        n, // number of nodes
        r, // replicas
        "MasterSlave", true); // do rebalance

    _controller = new MockController(_zkaddr, clusterName, "controller_0");
    _controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      _participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      _participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    _dataAccessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
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
