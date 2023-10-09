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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestCurrentStateComputationStage extends BaseStageTest {

  @Test
  public void testEmptyCS() {
    Map<String, Resource> resourceMap = getResourceMap();
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    ResourceControllerDataProvider dataCache = new ResourceControllerDataProvider();
    event.addAttribute(AttributeName.ControllerDataProvider.name(), dataCache);
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), new ClusterStatusMonitor(_clusterName));
    CurrentStateComputationStage stage = new CurrentStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    ClusterConfig clsCfg = dataCache.getClusterConfig();
    clsCfg.setInstanceCapacityKeys(ImmutableList.of("s1", "s2", "s3"));
    dataCache.setClusterConfig(clsCfg);
    dataCache.setInstanceConfigMap(ImmutableMap.of(
        "a", new InstanceConfig("a")
    ));
    runStage(event, stage);
    CurrentStateOutput output = event.getAttribute(AttributeName.CURRENT_STATE.name());
    AssertJUnit.assertEquals(
        output.getCurrentStateMap("testResourceName", new Partition("testResourceName_0")).size(),
        0);
  }

  @Test
  public void testSimpleCS() {
    // setup resource
    Map<String, Resource> resourceMap = getResourceMap();

    setupLiveInstances(5);

    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    ResourceControllerDataProvider dataCache = new ResourceControllerDataProvider();
    event.addAttribute(AttributeName.ControllerDataProvider.name(), dataCache);
    CurrentStateComputationStage stage = new CurrentStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output1 = event.getAttribute(AttributeName.CURRENT_STATE.name());
    AssertJUnit.assertEquals(
        output1.getCurrentStateMap("testResourceName", new Partition("testResourceName_0")).size(),
        0);

    // Add a state transition messages
    Message message = new Message(Message.MessageType.STATE_TRANSITION, "msg1");
    message.setFromState("OFFLINE");
    message.setToState("SLAVE");
    message.setResourceName("testResourceName");
    message.setPartitionName("testResourceName_1");
    message.setTgtName("localhost_3");
    message.setTgtSessionId("session_3");

    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.message("localhost_" + 3, message.getId()), message);

    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output2 = event.getAttribute(AttributeName.CURRENT_STATE.name());
    String pendingState =
        output2.getPendingMessage("testResourceName", new Partition("testResourceName_1"),
            "localhost_3").getToState();
    AssertJUnit.assertEquals(pendingState, "SLAVE");

    ZNRecord record1 = new ZNRecord("testResourceName");
    // Add a current state that matches sessionId and one that does not match
    CurrentState stateWithLiveSession = new CurrentState(record1);
    stateWithLiveSession.setSessionId("session_3");
    stateWithLiveSession.setStateModelDefRef("MasterSlave");
    stateWithLiveSession.setState("testResourceName_1", "OFFLINE");
    ZNRecord record2 = new ZNRecord("testResourceName");
    CurrentState stateWithDeadSession = new CurrentState(record2);
    stateWithDeadSession.setSessionId("session_dead");
    stateWithDeadSession.setStateModelDefRef("MasterSlave");
    stateWithDeadSession.setState("testResourceName_1", "MASTER");

    ZNRecord record3 = new ZNRecord("testTaskResourceName");
    CurrentState taskStateWithLiveSession = new CurrentState(record3);
    taskStateWithLiveSession.setSessionId("session_3");
    taskStateWithLiveSession.setStateModelDefRef("Task");
    taskStateWithLiveSession.setState("testTaskResourceName_1", "INIT");
    ZNRecord record4 = new ZNRecord("testTaskResourceName");
    CurrentState taskStateWithDeadSession = new CurrentState(record4);
    taskStateWithDeadSession.setSessionId("session_dead");
    taskStateWithDeadSession.setStateModelDefRef("Task");
    taskStateWithDeadSession.setState("testTaskResourceName_1", "INIT");

    accessor.setProperty(keyBuilder.currentState("localhost_3", "session_3", "testResourceName"),
        stateWithLiveSession);
    accessor.setProperty(keyBuilder.currentState("localhost_3", "session_dead", "testResourceName"),
        stateWithDeadSession);
    accessor.setProperty(
        keyBuilder.taskCurrentState("localhost_3", "session_3", "testTaskResourceName"),
        taskStateWithLiveSession);
    accessor.setProperty(
        keyBuilder.taskCurrentState("localhost_3", "session_dead", "testTaskResourceName"),
        taskStateWithDeadSession);

    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output3 = event.getAttribute(AttributeName.CURRENT_STATE.name());
    String currentState =
        output3.getCurrentState("testResourceName", new Partition("testResourceName_1"),
            "localhost_3");
    AssertJUnit.assertEquals(currentState, "OFFLINE");
    // Non Task Framework event will cause task current states to be ignored
    String taskCurrentState = output3
        .getCurrentState("testTaskResourceName", new Partition("testTaskResourceName_1"),
            "localhost_3");
    AssertJUnit.assertNull(taskCurrentState);

    // Add another state transition message which is stale
    message = new Message(Message.MessageType.STATE_TRANSITION, "msg2");
    message.setFromState("SLAVE");
    message.setToState("OFFLINE");
    message.setResourceName("testResourceName");
    message.setPartitionName("testResourceName_1");
    message.setTgtName("localhost_3");
    message.setTgtSessionId("session_3");
    accessor.setProperty(keyBuilder.message("localhost_" + 3, message.getId()), message);

    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output4 = event.getAttribute(AttributeName.CURRENT_STATE.name());
    AssertJUnit.assertEquals(dataCache.getStaleMessages().size(), 1);
    AssertJUnit.assertTrue(dataCache.getStaleMessages().containsKey("localhost_3"));
    AssertJUnit.assertTrue(dataCache.getStaleMessages().get("localhost_3").containsKey("msg2"));

    // Use a task event to check that task current states are included
    resourceMap = new HashMap<String, Resource>();
    Resource testTaskResource = new Resource("testTaskResourceName");
    testTaskResource.setStateModelDefRef("Task");
    testTaskResource.addPartition("testTaskResourceName_1");
    resourceMap.put("testTaskResourceName", testTaskResource);
    ClusterEvent taskEvent = new ClusterEvent(ClusterEventType.Unknown);
    taskEvent.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    taskEvent.addAttribute(AttributeName.ControllerDataProvider.name(),
        new WorkflowControllerDataProvider());
    runStage(taskEvent, new ReadClusterDataStage());
    runStage(taskEvent, stage);
    CurrentStateOutput output5 = taskEvent.getAttribute(AttributeName.CURRENT_STATE.name());
    taskCurrentState = output5
        .getCurrentState("testTaskResourceName", new Partition("testTaskResourceName_1"),
            "localhost_3");
    AssertJUnit.assertEquals(taskCurrentState, "INIT");
  }

}
