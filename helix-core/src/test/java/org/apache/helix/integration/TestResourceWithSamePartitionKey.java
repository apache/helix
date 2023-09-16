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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.common.execution.TestClusterParameters;
import org.apache.helix.common.execution.TestExecutionFlow;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @see HELIX-552
 *      StateModelFactory#_stateModelMap should use both resourceName and partitionKey to map a
 *      state model
 */
public class TestResourceWithSamePartitionKey extends ZkTestBase {

  @Test
  public void test() throws Exception {
    int nodeCount = 2;
    TestExecutionFlow flow = createClusterTestExecutionFlow();
    flow.createCluster(new TestClusterParameters.Builder()
        .setResourcesCount(1)
        .setPartitionsPerResource(2)
        .setNodeCount(nodeCount)
        .setReplicaCount(2)
        .setStateModelDef("OnlineOffline")
        .setRebalanceMode(RebalanceMode.CUSTOMIZED)
        .disableRebalance()
        .build());

    HelixDataAccessor accessor = new ZKHelixDataAccessor(flow.getClusterName(), new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setReplicas("2");
    idealState.setPartitionState("0", "localhost_12918", "ONLINE");
    idealState.setPartitionState("0", "localhost_12919", "ONLINE");
    idealState.setPartitionState("1", "localhost_12918", "ONLINE");
    idealState.setPartitionState("1", "localhost_12919", "ONLINE");
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    flow.startController()
        .initializeParticipants()
        .assertBestPossibleExternalViewVerifier();

    // add a second resource with the same partition-key
    IdealState newIdealState = new IdealState("TestDB1");
    newIdealState.getRecord().setSimpleFields(idealState.getRecord().getSimpleFields());
    newIdealState.setPartitionState("0", "localhost_12918", "ONLINE");
    newIdealState.setPartitionState("0", "localhost_12919", "ONLINE");
    newIdealState.setPartitionState("1", "localhost_12918", "ONLINE");
    newIdealState.setPartitionState("1", "localhost_12919", "ONLINE");
    accessor.setProperty(keyBuilder.idealStates("TestDB1"), newIdealState);

    flow.assertBestPossibleExternalViewVerifier();

    // assert no ERROR
    for (int i = 0; i < nodeCount; i++) {
      String instanceName = "localhost_" + (12918 + i);
      List<String> errs = accessor.getChildNames(keyBuilder.errors(instanceName));
      Assert.assertTrue(errs.isEmpty());
    }

    flow.cleanup();
  }

}
