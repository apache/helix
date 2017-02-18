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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPartitionMovementThrottle extends ZkStandAloneCMTestBase {
  ConfigAccessor _configAccessor;

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(_gZkClient);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // start dummy participants
    for (int i = 0; i < NODE_NR - 2; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      participant.setTransition(new DelayedTransition());
      participant.syncStart();
      _participants[i] = participant;
    }

    _configAccessor = new ConfigAccessor(_gZkClient);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
  }

  @Test()
  public void testResourceThrottle() throws Exception {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);

    StateTransitionThrottleConfig resourceThrottle =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.RESOURCE, 2);

    StateTransitionThrottleConfig clusterThrottle =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 100);

    clusterConfig.setStateTransitionThrottleConfigs(
        Arrays.asList(resourceThrottle, clusterThrottle));
    clusterConfig.setPersistIntermediateAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    List<String> dbs = new ArrayList<String>();

    for (int i = 0; i < 5; i++) {
      String dbName = "TestDB-" + i;
      _setupTool.addResourceToCluster(CLUSTER_NAME, dbName, 10, STATE_MODEL,
          RebalanceMode.FULL_AUTO + "");
      _setupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, _replica);
      dbs.add(dbName);
    }

    HelixClusterVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
    _clusterVerifier.verify(10000);

    DelayedTransition.setDelay(50);
    DelayedTransition.enableThrottleRecord();

    // add 2 nodes
    for (int i = NODE_NR - 2; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (1000 + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName.replace(':', '_'));
      participant.syncStart();
      _participants[i] = participant;
    }

    _clusterVerifier.verify(20000);

    for (String db : dbs) {
      validateThrottle(DelayedTransition.getResourcePatitionTransitionTimes(), db, 2);
    }
  }

  private void validateThrottle(
      Map<String, List<PartitionTransitionTime>> partitionTransitionTimesMap,
      String throttledItemName, int maxPendingTransition) {
    List<PartitionTransitionTime> pTimeList = partitionTransitionTimesMap.get(throttledItemName);

    Map<Long, List<PartitionTransitionTime>> startMap =
        new HashMap<Long, List<PartitionTransitionTime>>();
    Map<Long, List<PartitionTransitionTime>> endMap =
        new HashMap<Long, List<PartitionTransitionTime>>();
    List<Long> startEndPoints = new ArrayList<Long>();

    if (pTimeList == null) {
      System.out.println("no throttle result for :" + throttledItemName);
      return;
    }
    Collections.sort(pTimeList, new Comparator<PartitionTransitionTime>() {
      @Override
      public int compare(PartitionTransitionTime o1, PartitionTransitionTime o2) {
        return (int) (o1.start - o2.start);
      }
    });

    for (PartitionTransitionTime interval : pTimeList) {
      if (!startMap.containsKey(interval.start)) {
        startMap.put(interval.start, new ArrayList<PartitionTransitionTime>());
      }
      startMap.get(interval.start).add(interval);

      if (!endMap.containsKey(interval.end)) {
        endMap.put(interval.end, new ArrayList<PartitionTransitionTime>());
      }
      endMap.get(interval.end).add(interval);
      startEndPoints.add(interval.start);
      startEndPoints.add(interval.end);
    }

    Collections.sort(startEndPoints);

    List<PartitionTransitionTime> temp = new ArrayList<PartitionTransitionTime>();

    int maxInParallel = 0;
    for (long point : startEndPoints) {
      if (startMap.containsKey(point)) {
        temp.addAll(startMap.get(point));
      }
      int curSize = size(temp);
      if (curSize > maxInParallel) {
        maxInParallel = curSize;
      }
      if (endMap.containsKey(point)) {
        temp.removeAll(endMap.get(point));
      }
    }

    System.out.println(
        "MaxInParallel: " + maxInParallel + " maxPendingTransition: " + maxPendingTransition);
    Assert.assertTrue(maxInParallel <= maxPendingTransition,
        "Throttle condition does not meet for " + throttledItemName);
  }


  private int size(List<PartitionTransitionTime> timeList) {
    Set<String> partitions = new HashSet<String>();
    for (PartitionTransitionTime p : timeList) {
      partitions.add(p.partition);
    }
    return partitions.size();
  }

  private static class PartitionTransitionTime {
    String partition;
    long start;
    long end;

    public PartitionTransitionTime(String partition, long start, long end) {
      this.partition = partition;
      this.start = start;
      this.end = end;
    }

    @Override public String toString() {
      return "[" +
          "partition='" + partition + '\'' +
          ", start=" + start +
          ", end=" + end +
          ']';
    }
  }

  private static class DelayedTransition extends MockTransition {
    private static long _delay = 0;
    private static Map<String, List<PartitionTransitionTime>> resourcePatitionTransitionTimes =
        new HashMap<String, List<PartitionTransitionTime>>();
    private static Map<String, List<PartitionTransitionTime>> instancePatitionTransitionTimes =
        new HashMap<String, List<PartitionTransitionTime>>();
    private static boolean _recordThrottle = false;

    public static void setDelay(long delay) {
      _delay = delay;
    }

    public static Map<String, List<PartitionTransitionTime>> getResourcePatitionTransitionTimes() {
      return resourcePatitionTransitionTimes;
    }

    public static Map<String, List<PartitionTransitionTime>> getInstancePatitionTransitionTimes() {
      return instancePatitionTransitionTimes;
    }

    public static void enableThrottleRecord() {
      _recordThrottle = true;
    }

    @Override public void doTransition(Message message, NotificationContext context)
        throws InterruptedException {
      long start = System.currentTimeMillis();
      if (_delay > 0) {
        Thread.sleep(_delay);
      }
      long end = System.currentTimeMillis();
      if (_recordThrottle) {
        PartitionTransitionTime partitionTransitionTime =
            new PartitionTransitionTime(message.getPartitionName(), start, end);

        /*System.out.println(String
            .format("Transit resource %s partition %s from %s to %s at instance %s: %s",
                message.getResourceName(), message.getPartitionName(), message.getFromState(),
                message.getToState(), message.getTgtName(), partitionTransitionTime));
        */
        if (!resourcePatitionTransitionTimes.containsKey(message.getResourceName())) {
          resourcePatitionTransitionTimes
              .put(message.getResourceName(), new ArrayList<PartitionTransitionTime>());
        }
        resourcePatitionTransitionTimes.get(message.getResourceName()).add(partitionTransitionTime);

        if (!instancePatitionTransitionTimes.containsKey(message.getTgtName())) {
          instancePatitionTransitionTimes
              .put(message.getTgtName(), new ArrayList<PartitionTransitionTime>());
        }
        instancePatitionTransitionTimes.get(message.getTgtName()).add(partitionTransitionTime);
      }
    }
  }
}
