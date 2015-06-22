package org.apache.helix.integration;

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
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.TestHelper.Verifier;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.HelixTestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Test that node tagging behaves correctly in FULL_AUTO mode
 */
public class TestFullAutoNodeTagging extends ZkTestBase {
  private static final Logger LOG = Logger.getLogger(TestFullAutoNodeTagging.class);

  @Test
  public void testUntag() throws Exception {
    final int NUM_PARTICIPANTS = 2;
    final int NUM_PARTITIONS = 4;
    final int NUM_REPLICAS = 1;
    final String RESOURCE_NAME = "TestResource0";
    final String TAG = "ASSIGNABLE";

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestResource", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "OnlineOffline", RebalanceMode.FULL_AUTO, // use FULL_AUTO mode to test node tagging
        true); // do rebalance

    // Tag the resource
    final HelixAdmin helixAdmin = new ZKHelixAdmin(_zkclient);
    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, RESOURCE_NAME);
    idealState.setInstanceGroupTag(TAG);
    helixAdmin.setResourceIdealState(clusterName, RESOURCE_NAME, idealState);

    // Get a data accessor
    final HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // Tag the participants
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      final String instanceName = "localhost_" + (12918 + i);
      helixAdmin.addInstanceTag(clusterName, instanceName, TAG);
    }

    // Start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // Start participants
    MockParticipant[] participants = new MockParticipant[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    // Verify that there are NUM_PARTITIONS partitions in the external view, each having
    // NUM_REPLICAS replicas, where all assigned replicas are to tagged nodes, and they are all
    // ONLINE.
    Verifier v = new Verifier() {
      @Override
      public boolean verify() throws Exception {
        ExternalView externalView =
            HelixTestUtil.pollForProperty(ExternalView.class, accessor,
                keyBuilder.externalView(RESOURCE_NAME), true);
        if (externalView == null) {
          return false;
        }
        Set<String> taggedInstances =
            Sets.newHashSet(helixAdmin.getInstancesInClusterWithTag(clusterName, TAG));
        Set<String> partitionSet = externalView.getPartitionSet();
        if (partitionSet.size() != NUM_PARTITIONS) {
          return false;
        }
        for (String partitionName : partitionSet) {
          Map<String, String> stateMap = externalView.getStateMap(partitionName);
          if (stateMap.size() != NUM_REPLICAS) {
            return false;
          }
          for (String participantName : stateMap.keySet()) {
            if (!taggedInstances.contains(participantName)) {
              return false;
            }
            String state = stateMap.get(participantName);
            if (!state.equalsIgnoreCase("ONLINE")) {
              return false;
            }
          }
        }
        return true;
      }
    };

    // Run the verifier for both nodes tagged
    boolean initialResult = TestHelper.verify(v, 10 * 1000);
    Assert.assertTrue(initialResult);

    // Untag a node
    helixAdmin.removeInstanceTag(clusterName, "localhost_12918", TAG);

    // Verify again
    boolean finalResult = TestHelper.verify(v, 10 * 1000);
    Assert.assertTrue(finalResult);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Ensure that no assignments happen when there are no tagged nodes, but the resource is tagged
   */
  @Test
  public void testResourceTaggedFirst() throws Exception {
    final int NUM_PARTICIPANTS = 10;
    final int NUM_PARTITIONS = 4;
    final int NUM_REPLICAS = 2;
    final String RESOURCE_NAME = "TestDB0";
    final String TAG = "ASSIGNABLE";

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "MasterSlave", RebalanceMode.FULL_AUTO, // use FULL_AUTO mode to test node tagging
        true); // do rebalance

    // tag the resource
    HelixAdmin helixAdmin = new ZKHelixAdmin(_zkaddr);
    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, RESOURCE_NAME);
    idealState.setInstanceGroupTag(TAG);
    helixAdmin.setResourceIdealState(clusterName, RESOURCE_NAME, idealState);

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    Thread.sleep(1000);
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new HelixTestUtil.EmptyZkVerifier(clusterName,
            RESOURCE_NAME, _zkclient));
    Assert.assertTrue(result, "External view and current state must be empty");

    // cleanup
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      participants[i].syncStop();
    }
    controller.syncStop();
  }

  /**
   * Basic test for tagging behavior. 10 participants, of which 4 are tagged. Launch all 10,
   * checking external view every time a tagged node is started. Then shut down all 10, checking
   * external view every time a tagged node is killed.
   */
  @Test
  public void testSafeAssignment() throws Exception {
    final int NUM_PARTICIPANTS = 10;
    final int NUM_PARTITIONS = 4;
    final int NUM_REPLICAS = 2;
    final String RESOURCE_NAME = "TestDB0";
    final String TAG = "ASSIGNABLE";

    final String[] TAGGED_NODES = {
        "localhost_12920", "localhost_12922", "localhost_12924", "localhost_12925"
    };
    Set<String> taggedNodes = Sets.newHashSet(TAGGED_NODES);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "MasterSlave", RebalanceMode.FULL_AUTO, // use FULL_AUTO mode to test node tagging
        true); // do rebalance

    // tag the resource and participants
    HelixAdmin helixAdmin = new ZKHelixAdmin(_zkaddr);
    for (String taggedNode : TAGGED_NODES) {
      helixAdmin.addInstanceTag(clusterName, taggedNode, TAG);
    }
    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, RESOURCE_NAME);
    idealState.setInstanceGroupTag(TAG);
    helixAdmin.setResourceIdealState(clusterName, RESOURCE_NAME, idealState);

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();

      // ensure that everything is valid if this is a tagged node that is starting
      if (taggedNodes.contains(instanceName)) {
        // make sure that the best possible matches the external view
        Thread.sleep(500);
        boolean result =
            ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
        Assert.assertTrue(result);

        // make sure that the tagged state of the nodes is still balanced
        result =
            ClusterStateVerifier.verifyByZkCallback(new TaggedZkVerifier(clusterName,
                RESOURCE_NAME, TAGGED_NODES, false));
        Assert.assertTrue(result, "initial assignment with all tagged nodes live is invalid");
      }
    }

    // cleanup
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String participantName = participants[i].getInstanceName();
      participants[i].syncStop();
      if (taggedNodes.contains(participantName)) {
        // check that the external view is still correct even after removing tagged nodes
        taggedNodes.remove(participantName);
        Thread.sleep(500);
        boolean result =
            ClusterStateVerifier.verifyByZkCallback(new TaggedZkVerifier(clusterName,
                RESOURCE_NAME, TAGGED_NODES, taggedNodes.isEmpty()));
        Assert.assertTrue(result, "incorrect state after removing " + participantName + ", "
            + taggedNodes + " remain");
      }
    }
    controller.syncStop();
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Checker for basic validity of the external view given node tagging requirements
   */
  private static class TaggedZkVerifier extends ZkVerifier {
    private final String _resourceName;
    private final String[] _taggedNodes;
    private final boolean _isEmptyAllowed;

    /**
     * Create a verifier for a specific cluster and resource
     * @param clusterName the cluster to verify
     * @param resourceName the resource within the cluster to verify
     * @param taggedNodes nodes tagged with the resource tag
     * @param isEmptyAllowed true if empty assignments are legal
     */
    public TaggedZkVerifier(String clusterName, String resourceName, String[] taggedNodes,
        boolean isEmptyAllowed) {
      super(clusterName, _zkclient);
      _resourceName = resourceName;
      _taggedNodes = taggedNodes;
      _isEmptyAllowed = isEmptyAllowed;
    }

    @Override
    public boolean verify() {
      HelixDataAccessor accessor = new ZKHelixDataAccessor(getClusterName(), _baseAccessor);
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      ExternalView externalView = accessor.getProperty(keyBuilder.externalView(_resourceName));

      Set<String> taggedNodeSet = ImmutableSet.copyOf(_taggedNodes);

      // set up counts of partitions, masters, and slaves per node
      Map<String, Integer> partitionCount = Maps.newHashMap();
      int partitionSum = 0;
      Map<String, Integer> masterCount = Maps.newHashMap();
      int masterSum = 0;
      Map<String, Integer> slaveCount = Maps.newHashMap();
      int slaveSum = 0;

      for (String partitionName : externalView.getPartitionSet()) {
        Map<String, String> stateMap = externalView.getStateMap(partitionName);
        for (String participantName : stateMap.keySet()) {
          String state = stateMap.get(participantName);
          if (state.equalsIgnoreCase("MASTER") || state.equalsIgnoreCase("SLAVE")) {
            partitionSum++;
            incrementCount(partitionCount, participantName);
            if (!taggedNodeSet.contains(participantName)) {
              // not allowed to have a non-tagged node assigned
              LOG.error("Participant " + participantName + " is not tag, but has an assigned node");
              return false;
            } else if (state.equalsIgnoreCase("MASTER")) {
              masterSum++;
              incrementCount(masterCount, participantName);
            } else if (state.equalsIgnoreCase("SLAVE")) {
              slaveSum++;
              incrementCount(slaveCount, participantName);
            }
          }
        }
      }

      // check balance in partitions per node
      if (partitionCount.size() > 0) {
        boolean partitionMapDividesEvenly = partitionSum % partitionCount.size() == 0;
        boolean withinAverage =
            withinAverage(partitionCount, _isEmptyAllowed, partitionMapDividesEvenly);
        if (!withinAverage) {
          LOG.error("partition counts deviate from average");
          return false;
        }
      } else {
        if (!_isEmptyAllowed) {
          LOG.error("partition assignments are empty");
          return false;
        }
      }

      // check balance in masters per node
      if (masterCount.size() > 0) {
        boolean masterMapDividesEvenly = masterSum % masterCount.size() == 0;
        boolean withinAverage = withinAverage(masterCount, _isEmptyAllowed, masterMapDividesEvenly);
        if (!withinAverage) {
          LOG.error("master counts deviate from average");
          return false;
        }
      } else {
        if (!_isEmptyAllowed) {
          LOG.error("master assignments are empty");
          return false;
        }
      }

      // check balance in slaves per node
      if (slaveCount.size() > 0) {
        boolean slaveMapDividesEvenly = slaveSum % slaveCount.size() == 0;
        boolean withinAverage = withinAverage(slaveCount, true, slaveMapDividesEvenly);
        if (!withinAverage) {
          LOG.error("slave counts deviate from average");
          return false;
        }
      }
      return true;
    }

    private void incrementCount(Map<String, Integer> countMap, String key) {
      if (!countMap.containsKey(key)) {
        countMap.put(key, 0);
      }
      countMap.put(key, countMap.get(key) + 1);
    }

    private boolean withinAverage(Map<String, Integer> countMap, boolean isEmptyAllowed,
        boolean dividesEvenly) {
      if (countMap.size() == 0) {
        if (!isEmptyAllowed) {
          LOG.error("Map not allowed to be empty");
          return false;
        }
        return true;
      }
      int upperBound = 1;
      if (!dividesEvenly) {
        upperBound = 2;
      }
      int average = computeAverage(countMap);
      for (String participantName : countMap.keySet()) {
        int count = countMap.get(participantName);
        if (count < average - 1 || count > average + upperBound) {
          LOG.error("Count " + count + " for " + participantName + " too far from average of "
              + average);
          return false;
        }
      }
      return true;
    }

    private int computeAverage(Map<String, Integer> countMap) {
      if (countMap.size() == 0) {
        return -1;
      }
      int total = 0;
      for (int value : countMap.values()) {
        total += value;
      }
      return total / countMap.size();
    }
  }
}
