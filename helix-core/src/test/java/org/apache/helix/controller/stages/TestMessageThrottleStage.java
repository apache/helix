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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.testutil.HelixTestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMessageThrottleStage extends ZkTestBase {
  final String _className = "TestMessageThrottleStage";

  @Test
  public void testMsgThrottleBasic() throws Exception {
    String clusterName = "CLUSTER_" + _className + "_basic";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    TestHelper.setupEmptyCluster(_zkclient, clusterName);
    HelixManager manager = new DummyClusterManager(clusterName, accessor);

    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    List<IdealState> idealStates =
        HelixTestUtil.setupIdealState(_baseAccessor, clusterName, new int[] {
            0, 1
        }, new String[] {
          "TestDB"
        }, 1, 2);
    HelixTestUtil.setupLiveInstances(_baseAccessor, clusterName, new int[] {
        0, 1
    });
    HelixTestUtil.setupStateModel(_baseAccessor, clusterName);

    ClusterEvent event = new ClusterEvent("testEvent");
    event.addAttribute("helixmanager", manager);

    // get an empty best possible output for the partitions
    BestPossibleStateOutput bestPossOutput = getEmptyBestPossibleStateOutput(idealStates);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(), bestPossOutput);

    MessageThrottleStage throttleStage = new MessageThrottleStage();
    try {
      HelixTestUtil.runStage(event, throttleStage);
      Assert.fail("Should throw exception since DATA_CACHE is null");
    } catch (Exception e) {
      // OK
    }

    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    HelixTestUtil.runPipeline(event, dataRefresh);

    try {
      HelixTestUtil.runStage(event, throttleStage);
      Assert.fail("Should throw exception since RESOURCE is null");
    } catch (Exception e) {
      // OK
    }
    HelixTestUtil.runStage(event, new ResourceComputationStage());

    try {
      HelixTestUtil.runStage(event, throttleStage);
      Assert.fail("Should throw exception since MESSAGE_SELECT is null");
    } catch (Exception e) {
      // OK
    }
    MessageOutput msgSelectOutput = new MessageOutput();
    List<Message> selectMessages = new ArrayList<Message>();
    Message msg =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-001"),
            "OFFLINE", "SLAVE", "TestDB", "localhost_0");
    selectMessages.add(msg);

    msgSelectOutput.setMessages(ResourceId.from("TestDB"), PartitionId.from("TestDB_0"),
        selectMessages);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.toString(), msgSelectOutput);

    HelixTestUtil.runStage(event, throttleStage);

    MessageOutput msgThrottleOutput =
        event.getAttribute(AttributeName.MESSAGES_THROTTLE.toString());
    Assert.assertEquals(
        msgThrottleOutput.getMessages(ResourceId.from("TestDB"), PartitionId.from("TestDB_0"))
            .size(), 1);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test()
  public void testMsgThrottleConstraints() throws Exception {
    String clusterName = "CLUSTER_" + _className + "_constraints";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    TestHelper.setupEmptyCluster(_zkclient, clusterName);

    HelixManager manager = new DummyClusterManager(clusterName, accessor);

    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    List<IdealState> idealStates =
        HelixTestUtil.setupIdealState(_baseAccessor, clusterName, new int[] {
            0, 1
        }, new String[] {
          "TestDB"
        }, 1, 2);
    HelixTestUtil.setupLiveInstances(_baseAccessor, clusterName, new int[] {
        0, 1
    });
    HelixTestUtil.setupStateModel(_baseAccessor, clusterName);

    // setup constraints
    ZNRecord record = new ZNRecord(ConstraintType.MESSAGE_CONSTRAINT.toString());

    // constraint0:
    // "MESSAGE_TYPE=STATE_TRANSITION,CONSTRAINT_VALUE=ANY"
    record.setMapField("constraint0", new TreeMap<String, String>());
    record.getMapField("constraint0").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint0").put("CONSTRAINT_VALUE", "ANY");
    ConstraintItem constraint0 = new ConstraintItem(record.getMapField("constraint0"));

    // constraint1:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,CONSTRAINT_VALUE=ANY"
    record.setMapField("constraint1", new TreeMap<String, String>());
    record.getMapField("constraint1").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint1").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint1").put("CONSTRAINT_VALUE", "50");
    ConstraintItem constraint1 = new ConstraintItem(record.getMapField("constraint1"));

    // constraint2:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=.*,RESOURCE=TestDB,CONSTRAINT_VALUE=2";
    record.setMapField("constraint2", new TreeMap<String, String>());
    record.getMapField("constraint2").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint2").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint2").put("INSTANCE", ".*");
    record.getMapField("constraint2").put("RESOURCE", "TestDB");
    record.getMapField("constraint2").put("CONSTRAINT_VALUE", "2");
    ConstraintItem constraint2 = new ConstraintItem(record.getMapField("constraint2"));

    // constraint3:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=localhost_12918,RESOURCE=.*,CONSTRAINT_VALUE=1";
    record.setMapField("constraint3", new TreeMap<String, String>());
    record.getMapField("constraint3").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint3").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint3").put("INSTANCE", "localhost_1");
    record.getMapField("constraint3").put("RESOURCE", ".*");
    record.getMapField("constraint3").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint3 = new ConstraintItem(record.getMapField("constraint3"));

    // constraint4:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=.*,RESOURCE=.*,CONSTRAINT_VALUE=10"
    record.setMapField("constraint4", new TreeMap<String, String>());
    record.getMapField("constraint4").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint4").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint4").put("INSTANCE", ".*");
    record.getMapField("constraint4").put("RESOURCE", ".*");
    record.getMapField("constraint4").put("CONSTRAINT_VALUE", "10");
    ConstraintItem constraint4 = new ConstraintItem(record.getMapField("constraint4"));

    // constraint5:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=localhost_12918,RESOURCE=TestDB,CONSTRAINT_VALUE=5"
    record.setMapField("constraint5", new TreeMap<String, String>());
    record.getMapField("constraint5").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint5").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint5").put("INSTANCE", "localhost_0");
    record.getMapField("constraint5").put("RESOURCE", "TestDB");
    record.getMapField("constraint5").put("CONSTRAINT_VALUE", "3");
    ConstraintItem constraint5 = new ConstraintItem(record.getMapField("constraint5"));

    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()),
        new ClusterConstraints(record));

    // ClusterConstraints constraint =
    // accessor.getProperty(ClusterConstraints.class,
    // PropertyType.CONFIGS,
    // ConfigScopeProperty.CONSTRAINT.toString(),
    // ConstraintType.MESSAGE_CONSTRAINT.toString());
    ClusterConstraints constraint =
        accessor.getProperty(keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()));

    MessageThrottleStage throttleStage = new MessageThrottleStage();

    // test constraintSelection
    // message1: hit contraintSelection rule1 and rule2
    Message msg1 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-001"),
            "OFFLINE", "SLAVE", "TestDB", "localhost_0");

    Map<ConstraintAttribute, String> msgAttr = ClusterConstraints.toConstraintAttributes(msg1);
    Set<ConstraintItem> matches = constraint.match(msgAttr);
    System.out.println(msg1 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 5);
    Assert.assertTrue(containsConstraint(matches, constraint0));
    Assert.assertTrue(containsConstraint(matches, constraint1));
    Assert.assertTrue(containsConstraint(matches, constraint2));
    Assert.assertTrue(containsConstraint(matches, constraint4));
    Assert.assertTrue(containsConstraint(matches, constraint5));

    matches = throttleStage.selectConstraints(matches, msgAttr);
    System.out.println(msg1 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 2);
    Assert.assertTrue(containsConstraint(matches, constraint1));
    Assert.assertTrue(containsConstraint(matches, constraint5));

    // message2: hit contraintSelection rule1, rule2, and rule3
    Message msg2 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-002"),
            "OFFLINE", "SLAVE", "TestDB", "localhost_1");

    msgAttr = ClusterConstraints.toConstraintAttributes(msg2);
    matches = constraint.match(msgAttr);
    System.out.println(msg2 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 5);
    Assert.assertTrue(containsConstraint(matches, constraint0));
    Assert.assertTrue(containsConstraint(matches, constraint1));
    Assert.assertTrue(containsConstraint(matches, constraint2));
    Assert.assertTrue(containsConstraint(matches, constraint3));
    Assert.assertTrue(containsConstraint(matches, constraint4));

    matches = throttleStage.selectConstraints(matches, msgAttr);
    System.out.println(msg2 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 2);
    Assert.assertTrue(containsConstraint(matches, constraint1));
    Assert.assertTrue(containsConstraint(matches, constraint3));

    // test messageThrottleStage
    ClusterEvent event = new ClusterEvent("testEvent");
    event.addAttribute("helixmanager", manager);

    // get an empty best possible output for the partitions
    BestPossibleStateOutput bestPossOutput = getEmptyBestPossibleStateOutput(idealStates);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(), bestPossOutput);

    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    HelixTestUtil.runPipeline(event, dataRefresh);
    HelixTestUtil.runStage(event, new ResourceComputationStage());
    MessageOutput msgSelectOutput = new MessageOutput();

    Message msg3 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-003"),
            "OFFLINE", "SLAVE", "TestDB", "localhost_0");

    Message msg4 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-004"),
            "OFFLINE", "SLAVE", "TestDB", "localhost_0");

    Message msg5 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-005"),
            "OFFLINE", "SLAVE", "TestDB", "localhost_0");

    Message msg6 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-006"),
            "OFFLINE", "SLAVE", "TestDB", "localhost_1");

    List<Message> selectMessages = new ArrayList<Message>();
    selectMessages.add(msg1);
    selectMessages.add(msg2);
    selectMessages.add(msg3);
    selectMessages.add(msg4);
    selectMessages.add(msg5); // should be throttled
    selectMessages.add(msg6); // should be throttled

    msgSelectOutput.setMessages(ResourceId.from("TestDB"), PartitionId.from("TestDB_0"),
        selectMessages);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.toString(), msgSelectOutput);

    HelixTestUtil.runStage(event, throttleStage);

    MessageOutput msgThrottleOutput =
        event.getAttribute(AttributeName.MESSAGES_THROTTLE.toString());
    List<Message> throttleMessages =
        msgThrottleOutput.getMessages(ResourceId.from("TestDB"), PartitionId.from("TestDB_0"));
    Assert.assertEquals(throttleMessages.size(), 4);
    Assert.assertTrue(throttleMessages.contains(msg1));
    Assert.assertTrue(throttleMessages.contains(msg2));
    Assert.assertTrue(throttleMessages.contains(msg3));
    Assert.assertTrue(throttleMessages.contains(msg4));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test()
  public void testMsgThrottleConstraintsQuota() throws Exception {
    String clusterName = "CLUSTER_" + _className + "_constraints_quota";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    TestHelper.setupEmptyCluster(_zkclient, clusterName);

    HelixManager manager = new DummyClusterManager(clusterName, accessor);

    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    List<IdealState> idealStates =
        HelixTestUtil.setupIdealState(_baseAccessor, clusterName, new int[] {
            0, 1
        }, new String[] {
          "TestDB"
        }, 2, 2);
    HelixTestUtil.setupLiveInstances(_baseAccessor, clusterName, new int[] {
        0, 1
    });
    HelixTestUtil.setupStateModel(_baseAccessor, clusterName);

    // setup constraints
    ZNRecord record = new ZNRecord(ConstraintType.MESSAGE_CONSTRAINT.toString());

    // constraint 0 & 1, per instance constraint
    record.setMapField("constraint0", new TreeMap<String, String>());
    record.getMapField("constraint0").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint0").put("INSTANCE", "localhost_0");
    record.getMapField("constraint0").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint0 = new ConstraintItem(record.getMapField("constraint0"));

    record.setMapField("constraint1", new TreeMap<String, String>());
    record.getMapField("constraint1").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint1").put("INSTANCE", "localhost_1");
    record.getMapField("constraint1").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint1 = new ConstraintItem(record.getMapField("constraint1"));

    // constraint 2 & 3, per partition constraint
    record.setMapField("constraint2", new TreeMap<String, String>());
    record.getMapField("constraint2").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint2").put("PARTITION", "TestDB_0");
    record.getMapField("constraint2").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint2 = new ConstraintItem(record.getMapField("constraint2"));

    record.setMapField("constraint3", new TreeMap<String, String>());
    record.getMapField("constraint3").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint3").put("PARTITION", "TestDB_1");
    record.getMapField("constraint3").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint3 = new ConstraintItem(record.getMapField("constraint1"));

    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()),
        new ClusterConstraints(record));

    // ClusterConstraints constraint =
    // accessor.getProperty(ClusterConstraints.class,
    // PropertyType.CONFIGS,
    // ConfigScopeProperty.CONSTRAINT.toString(),
    // ConstraintType.MESSAGE_CONSTRAINT.toString());
    ClusterConstraints constraint =
        accessor.getProperty(keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()));

    MessageThrottleStage throttleStage = new MessageThrottleStage();

    // test messageThrottleStage
    ClusterEvent event = new ClusterEvent("testEvent");
    event.addAttribute("helixmanager", manager);

    // get an empty best possible output for the partitions
    BestPossibleStateOutput bestPossOutput = getEmptyBestPossibleStateOutput(idealStates);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(), bestPossOutput);

    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());
    HelixTestUtil.runPipeline(event, dataRefresh);
    HelixTestUtil.runStage(event, new ResourceComputationStage());
    MessageOutput msgSelectOutput = new MessageOutput();

    Message msg1 =
            HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-001"),
                "OFFLINE", "SLAVE", "TestDB", "localhost_0", "TestDB_0");

    Message msg2 =
            HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-002"),
                "OFFLINE", "SLAVE", "TestDB", "localhost_0", "TestDB_1");

    Message msg3 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-003"),
            "OFFLINE", "SLAVE", "TestDB", "localhost_1", "TestDB_0");

    Message msg4 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-004"),
            "OFFLINE", "SLAVE", "TestDB", "localhost_1", "TestDB_1");

    List<Message> selectMessages0 = new ArrayList<Message>();
    selectMessages0.add(msg1);
    selectMessages0.add(msg2);
    List<Message> selectMessages1 = new ArrayList<Message>();
    selectMessages1.add(msg3);
    selectMessages1.add(msg4);

    msgSelectOutput.setMessages(ResourceId.from("TestDB"), PartitionId.from("TestDB_0"),
        selectMessages0);
    msgSelectOutput.setMessages(ResourceId.from("TestDB"), PartitionId.from("TestDB_1"),
        selectMessages1);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.toString(), msgSelectOutput);

    HelixTestUtil.runStage(event, throttleStage);

    MessageOutput msgThrottleOutput =
        event.getAttribute(AttributeName.MESSAGES_THROTTLE.toString());
    List<Message> throttleMessages =
        msgThrottleOutput.getMessages(ResourceId.from("TestDB"), PartitionId.from("TestDB_0"));
    Assert.assertEquals(throttleMessages.size(), 1);
    Assert.assertTrue(throttleMessages.contains(msg1));

    throttleMessages = msgThrottleOutput.getMessages(ResourceId.from("TestDB"), PartitionId.from("TestDB_1"));
    Assert.assertEquals(throttleMessages.size(), 1);
    Assert.assertTrue(throttleMessages.contains(msg4));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  private boolean containsConstraint(Set<ConstraintItem> constraints, ConstraintItem constraint) {
    for (ConstraintItem item : constraints) {
      if (item.toString().equals(constraint.toString())) {
        return true;
      }
    }
    return false;
  }

  private BestPossibleStateOutput getEmptyBestPossibleStateOutput(List<IdealState> idealStates) {
    BestPossibleStateOutput output = new BestPossibleStateOutput();
    for (IdealState idealState : idealStates) {
      ResourceId resourceId = idealState.getResourceId();
      ResourceAssignment assignment = new ResourceAssignment(resourceId);
      for (PartitionId partitionId : idealState.getPartitionIdSet()) {
        Map<ParticipantId, State> emptyMap = Collections.emptyMap();
        assignment.addReplicaMap(partitionId, emptyMap);
      }
      output.setResourceAssignment(resourceId, assignment);
    }
    return output;
  }
  // add pending message test case

}
