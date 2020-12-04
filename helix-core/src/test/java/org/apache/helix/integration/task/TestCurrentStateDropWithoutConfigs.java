package org.apache.helix.integration.task;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskPartitionState;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test to make sure that current states of jobs are dropped if no JobConfig exists
 */
public class TestCurrentStateDropWithoutConfigs extends TaskTestBase {
  protected HelixDataAccessor _accessor;

  @Test
  public void testCurrentStateDropWithoutConfigs() throws Exception {
    String jobName = TestHelper.getTestMethodName() + "_0";
    String taskName = jobName + "_0";

    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    LiveInstance liveInstance = _accessor
        .getProperty(_accessor.keyBuilder().liveInstance(_participants[0].getInstanceName()));
    CurrentState currentState = new CurrentState(jobName);
    currentState.setSessionId(liveInstance.getEphemeralOwner());
    currentState.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    currentState.setState(taskName, TaskPartitionState.RUNNING.name());
    currentState.setPreviousState(taskName, TaskPartitionState.INIT.name());
    currentState.setStartTime(taskName, System.currentTimeMillis());
    currentState.setEndTime(taskName, System.currentTimeMillis());
    _accessor.setProperty(_accessor.keyBuilder()
        .taskCurrentState(_participants[0].getInstanceName(), liveInstance.getEphemeralOwner(),
            jobName), currentState);

    Assert.assertTrue(TestHelper.verify(() -> _accessor.getProperty(_accessor.keyBuilder()
        .taskCurrentState(_participants[0].getInstanceName(), liveInstance.getEphemeralOwner(),
            jobName)) == null, TestHelper.WAIT_DURATION * 10));
  }
}
