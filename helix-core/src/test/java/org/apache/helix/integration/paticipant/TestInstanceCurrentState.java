package org.apache.helix.integration.paticipant;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestInstanceCurrentState extends TaskTestBase {
  private long _testStartTime;
  @BeforeClass
  public void beforeClass() throws Exception {
    _testStartTime = System.currentTimeMillis();
    setSingleTestEnvironment();
    super.beforeClass();
  }

  @Test public void testAddedFieldsInCurrentState() {
    String instanceName = PARTICIPANT_PREFIX + "_" + _startPort;
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    LiveInstance liveInstance =
        accessor.getProperty(accessor.keyBuilder().liveInstance(instanceName));
    CurrentState currentState = accessor.getProperty(accessor.keyBuilder()
        .currentState(instanceName, liveInstance.getEphemeralOwner(), WorkflowGenerator.DEFAULT_TGT_DB));
    // Test start time should happen after test start time
    Assert.assertTrue(
        currentState.getStartTime(WorkflowGenerator.DEFAULT_TGT_DB + "_0") >= _testStartTime);

    // Test end time is always larger than start time
    Assert.assertTrue(
        currentState.getEndTime(WorkflowGenerator.DEFAULT_TGT_DB + "_0") >= currentState
            .getStartTime(WorkflowGenerator.DEFAULT_TGT_DB + "_0"));

    // Final state is MASTER, so SLAVE will be the previous state
    Assert.assertEquals(currentState.getPreviousState(WorkflowGenerator.DEFAULT_TGT_DB + "_0"),
        "SLAVE");
  }
}
