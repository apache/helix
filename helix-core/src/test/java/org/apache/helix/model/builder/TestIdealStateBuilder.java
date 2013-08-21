package org.apache.helix.model.builder;

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

import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestIdealStateBuilder {
  @Test
  public void testAutoISBuilder() {
    AutoModeISBuilder builder = new AutoModeISBuilder("test-db");
    builder.setStateModel("MasterSlave").setNumPartitions(2).setNumReplica(2);
    builder.assignPreferenceList("test-db_0", "node_0", "node_1").assignPreferenceList("test-db_1",
        "node_1", "node_0");

    IdealState idealState = null;
    try {
      idealState = builder.build();
    } catch (Exception e) {
      Assert.fail("fail to build an auto mode ideal-state.", e);
    }
    // System.out.println("ideal-state: " + idealState);
    Assert.assertEquals(idealState.getRebalanceMode(), IdealState.RebalanceMode.SEMI_AUTO,
        "rebalancer mode should be semi-auto");
  }

  @Test
  public void testAutoRebalanceISModeBuilder() {
    AutoRebalanceModeISBuilder builder = new AutoRebalanceModeISBuilder("test-db");
    builder.setStateModel("MasterSlave").setNumPartitions(2).setNumReplica(2);
    builder.add("test-db_0").add("test-db_1");

    IdealState idealState = null;
    try {
      idealState = builder.build();
    } catch (Exception e) {
      Assert.fail("fail to build an auto-rebalance mode ideal-state.", e);
    }
    // System.out.println("ideal-state: " + idealState);
    Assert.assertEquals(idealState.getRebalanceMode(), IdealState.RebalanceMode.FULL_AUTO,
        "rebalancer mode should be auto");

  }

  @Test
  public void testCustomModeISBuilder() {
    CustomModeISBuilder builder = new CustomModeISBuilder("test-db");
    builder.setStateModel("MasterSlave").setNumPartitions(2).setNumReplica(2);
    builder.assignInstanceAndState("test-db_0", "node_0", "MASTER")
        .assignInstanceAndState("test-db_0", "node_1", "SLAVE")
        .assignInstanceAndState("test-db_1", "node_0", "SLAVE")
        .assignInstanceAndState("test-db_1", "node_1", "MASTER");

    IdealState idealState = null;
    try {
      idealState = builder.build();
    } catch (Exception e) {
      Assert.fail("fail to build a custom mode ideal-state.", e);
    }
    // System.out.println("ideal-state: " + idealState);
    Assert.assertEquals(idealState.getRebalanceMode(), IdealState.RebalanceMode.CUSTOMIZED,
        "rebalancer mode should be customized");

  }
}
