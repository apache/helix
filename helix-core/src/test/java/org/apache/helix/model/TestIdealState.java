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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
public class TestIdealState {
  @Test
  public void testGetInstanceSet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;
    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    IdealState idealState = new IdealState("idealState");
    idealState.getRecord().setListField("TestDB_0", Arrays.asList("node_1", "node_2"));
    Map<String, String> instanceState = new HashMap<String, String>();
    instanceState.put("node_3", "MASTER");
    instanceState.put("node_4", "SLAVE");
    idealState.getRecord().setMapField("TestDB_1", instanceState);

    // test SEMI_AUTO mode
    idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    Set<ParticipantId> instances = idealState.getParticipantSet(PartitionId.from("TestDB_0"));
    // System.out.println("instances: " + instances);
    Assert.assertEquals(instances.size(), 2, "Should contain node_1 and node_2");
    Assert.assertTrue(instances.contains(ParticipantId.from("node_1")),
        "Should contain node_1 and node_2");
    Assert.assertTrue(instances.contains(ParticipantId.from("node_2")),
        "Should contain node_1 and node_2");

    instances = idealState.getParticipantSet(PartitionId.from("TestDB_nonExist_auto"));
    Assert.assertEquals(instances, Collections.emptySet(), "Should get empty set");

    // test CUSTOMIZED mode
    idealState.setRebalanceMode(RebalanceMode.CUSTOMIZED);
    instances = idealState.getParticipantSet(PartitionId.from("TestDB_1"));
    // System.out.println("instances: " + instances);
    Assert.assertEquals(instances.size(), 2, "Should contain node_3 and node_4");
    Assert.assertTrue(instances.contains(ParticipantId.from("node_3")),
        "Should contain node_3 and node_4");
    Assert.assertTrue(instances.contains(ParticipantId.from("node_4")),
        "Should contain node_3 and node_4");

    instances = idealState.getParticipantSet(PartitionId.from("TestDB_nonExist_custom"));
    Assert.assertEquals(instances, Collections.emptySet(), "Should get empty set");

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testReplicas() {
    IdealState idealState = new IdealState("test-db");
    idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    idealState.setNumPartitions(4);
    idealState.setStateModelDefId(StateModelDefId.from("MasterSlave"));

    idealState.setReplicas("" + 2);

    List<String> preferenceList = new ArrayList<String>();
    preferenceList.add("node_0");
    idealState.getRecord().setListField("test-db_0", preferenceList);
    Assert.assertFalse(idealState.isValid(),
        "should fail since replicas not equals to preference-list size");

    preferenceList.add("node_1");
    idealState.getRecord().setListField("test-db_0", preferenceList);
    Assert.assertTrue(idealState.isValid(),
        "should pass since replicas equals to preference-list size");
  }

  @Test
  public void testFullAutoModeCompatibility() {
    IdealState idealStateOld = new IdealState("old-test-db");
    idealStateOld.setIdealStateMode(IdealStateModeProperty.AUTO_REBALANCE.toString());
    Assert.assertEquals(idealStateOld.getRebalanceMode(), RebalanceMode.FULL_AUTO);
    Assert.assertEquals(idealStateOld.getIdealStateMode(), IdealStateModeProperty.AUTO_REBALANCE);

    IdealState idealStateNew = new IdealState("new-test-db");
    idealStateNew.setRebalanceMode(RebalanceMode.FULL_AUTO);
    Assert.assertEquals(idealStateNew.getIdealStateMode(), IdealStateModeProperty.AUTO_REBALANCE);
    Assert.assertEquals(idealStateNew.getRebalanceMode(), RebalanceMode.FULL_AUTO);
  }

  @Test
  public void testSemiAutoModeCompatibility() {
    IdealState idealStateOld = new IdealState("old-test-db");
    idealStateOld.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
    Assert.assertEquals(idealStateOld.getRebalanceMode(), RebalanceMode.SEMI_AUTO);
    Assert.assertEquals(idealStateOld.getIdealStateMode(), IdealStateModeProperty.AUTO);

    IdealState idealStateNew = new IdealState("new-test-db");
    idealStateNew.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    Assert.assertEquals(idealStateNew.getIdealStateMode(), IdealStateModeProperty.AUTO);
    Assert.assertEquals(idealStateNew.getRebalanceMode(), RebalanceMode.SEMI_AUTO);
  }

  @Test
  public void testCustomizedModeCompatibility() {
    IdealState idealStateOld = new IdealState("old-test-db");
    idealStateOld.setIdealStateMode(IdealStateModeProperty.CUSTOMIZED.toString());
    Assert.assertEquals(idealStateOld.getRebalanceMode(), RebalanceMode.CUSTOMIZED);
    Assert.assertEquals(idealStateOld.getIdealStateMode(), IdealStateModeProperty.CUSTOMIZED);

    IdealState idealStateNew = new IdealState("new-test-db");
    idealStateNew.setRebalanceMode(RebalanceMode.CUSTOMIZED);
    Assert.assertEquals(idealStateNew.getIdealStateMode(), IdealStateModeProperty.CUSTOMIZED);
    Assert.assertEquals(idealStateNew.getRebalanceMode(), RebalanceMode.CUSTOMIZED);
  }
}
