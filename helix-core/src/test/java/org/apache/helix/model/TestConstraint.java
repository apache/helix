package org.apache.helix.model;

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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.testutil.HelixTestUtil;
import org.apache.helix.testutil.TestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConstraint extends ZkTestBase {

  @Test
  public void testMsgConstraint() {
    String className = TestUtil.getTestName();
    System.out.println("START testMsgConstraint() at " + new Date(System.currentTimeMillis()));

    String clusterName = "CLUSTER_" + className + "_msg";
    TestHelper.setupEmptyCluster(_zkclient, clusterName);
    ZNRecord record = new ZNRecord("testMsgConstraint");

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
    record.getMapField("constraint3").put("INSTANCE", "localhost_12919");
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
    record.getMapField("constraint5").put("INSTANCE", "localhost_12918");
    record.getMapField("constraint5").put("RESOURCE", "TestDB");
    record.getMapField("constraint5").put("CONSTRAINT_VALUE", "5");
    ConstraintItem constraint5 = new ConstraintItem(record.getMapField("constraint5"));

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkclient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.setProperty(keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()),
        new ClusterConstraints(record));

    record =
        accessor.getProperty(keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()))
            .getRecord();
    ClusterConstraints constraint = new ClusterConstraints(record);
    // System.out.println("constraint: " + constraint);

    // message1
    Message msg1 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-001"), "OFFLINE",
            "SLAVE", "TestDB", "localhost_12918");

    Map<ConstraintAttribute, String> msgAttr = ClusterConstraints.toConstraintAttributes(msg1);
    Set<ConstraintItem> matches = constraint.match(msgAttr);
    System.out.println(msg1 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 5);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint1));
    Assert.assertTrue(contains(matches, constraint2));
    Assert.assertTrue(contains(matches, constraint4));
    Assert.assertTrue(contains(matches, constraint5));

    // message2
    Message msg2 =
        HelixTestUtil.newMessage(MessageType.STATE_TRANSITION, MessageId.from("msgId-002"), "OFFLINE",
            "SLAVE", "TestDB", "localhost_12919");

    msgAttr = ClusterConstraints.toConstraintAttributes(msg2);
    matches = constraint.match(msgAttr);
    System.out.println(msg2 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 5);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint1));
    Assert.assertTrue(contains(matches, constraint2));
    Assert.assertTrue(contains(matches, constraint3));
    Assert.assertTrue(contains(matches, constraint4));

    System.out.println("END testMsgConstraint() at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testStateConstraint() {
    String className = TestUtil.getTestName();
    System.out.println("START testStateConstraint() at " + new Date(System.currentTimeMillis()));

    String clusterName = "CLUSTER_" + className + "_state";
    TestHelper.setupEmptyCluster(_zkclient, clusterName);
    ZNRecord record = new ZNRecord("testStateConstraint");

    // constraint0:
    // "STATE=MASTER,CONSTRAINT_VALUE=1"
    record.setMapField("constraint0", new TreeMap<String, String>());
    record.getMapField("constraint0").put("STATE", "MASTER");
    record.getMapField("constraint0").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint0 = new ConstraintItem(record.getMapField("constraint0"));

    // constraint1:
    // "STATE=MASTER,RESOURCE=TestDB,CONSTRAINT_VALUE=5"
    record.setMapField("constraint1", new TreeMap<String, String>());
    record.getMapField("constraint1").put("STATE", "MASTER");
    record.getMapField("constraint1").put("RESOURCE", "TestDB");
    record.getMapField("constraint1").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint1 = new ConstraintItem(record.getMapField("constraint1"));

    // constraint2:
    // "STATE=MASTER,RESOURCE=.*,CONSTRAINT_VALUE=2"
    record.setMapField("constraint2", new TreeMap<String, String>());
    record.getMapField("constraint2").put("STATE", "MASTER");
    record.getMapField("constraint2").put("RESOURCE", ".*");
    record.getMapField("constraint2").put("CONSTRAINT_VALUE", "2");
    ConstraintItem constraint2 = new ConstraintItem(record.getMapField("constraint2"));

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkclient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.setProperty(keyBuilder.constraint(ConstraintType.STATE_CONSTRAINT.toString()),
        new ClusterConstraints(record));

    record =
        accessor.getProperty(keyBuilder.constraint(ConstraintType.STATE_CONSTRAINT.toString()))
            .getRecord();
    ClusterConstraints constraint = new ClusterConstraints(record);
    // System.out.println("constraint: " + constraint);

    // state1: hit rule2
    Map<ConstraintAttribute, String> stateAttr1 = new HashMap<ConstraintAttribute, String>();
    stateAttr1.put(ConstraintAttribute.STATE, "MASTER");
    stateAttr1.put(ConstraintAttribute.RESOURCE, "TestDB");

    Set<ConstraintItem> matches = constraint.match(stateAttr1);
    System.out.println(stateAttr1 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 3);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint1));
    Assert.assertTrue(contains(matches, constraint2));

    // matches = selectConstraints(matches, stateAttr1);
    // System.out.println(stateAttr1 + " matches(" + matches.size() + "): " + matches);
    // Assert.assertEquals(matches.size(), 2);
    // Assert.assertTrue(contains(matches, constraint0));
    // Assert.assertTrue(contains(matches, constraint1));

    // state2: not hit any rules
    Map<ConstraintAttribute, String> stateAttr2 = new HashMap<ConstraintAttribute, String>();
    stateAttr2.put(ConstraintAttribute.STATE, "MASTER");
    stateAttr2.put(ConstraintAttribute.RESOURCE, "MyDB");

    matches = constraint.match(stateAttr2);
    System.out.println(stateAttr2 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 2);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint2));

    // matches = selectConstraints(matches, stateAttr2);
    // System.out.println(stateAttr2 + " matches(" + matches.size() + "): " + matches);
    // Assert.assertEquals(matches.size(), 2);
    // Assert.assertTrue(contains(matches, constraint0));
    // Assert.assertTrue(contains(matches, constraint2));

    System.out.println("END testStateConstraint() at " + new Date(System.currentTimeMillis()));
  }

  private boolean contains(Set<ConstraintItem> constraints, ConstraintItem constraint) {
    for (ConstraintItem item : constraints) {
      if (item.toString().equals(constraint.toString())) {
        return true;
      }
    }
    return false;
  }
}
