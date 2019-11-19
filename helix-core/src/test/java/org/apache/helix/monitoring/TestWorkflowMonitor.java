package org.apache.helix.monitoring;

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

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.WorkflowMonitor;
import org.apache.helix.task.TaskState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestWorkflowMonitor {
  private static final String TEST_CLUSTER_NAME = "TestCluster";
  private static final String TEST_WORKFLOW_TYPE = "WorkflowTestType";
  private static final String TEST_WORKFLOW_MBEAN_NAME = String
      .format("%s=%s, %s=%s", "cluster", TEST_CLUSTER_NAME, "workflowType", TEST_WORKFLOW_TYPE);
  private static final MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

  @Test
  public void testRun() throws Exception {
    WorkflowMonitor wm = new WorkflowMonitor(TEST_CLUSTER_NAME, TEST_WORKFLOW_TYPE);
    registerMbean(wm, getObjectName());
    Set<ObjectInstance> existingInstances = beanServer.queryMBeans(
        new ObjectName(MonitorDomainNames.ClusterStatus.name() + ":" + TEST_WORKFLOW_MBEAN_NAME),
        null);
    HashSet<String> expectedAttr = new HashSet<>(Arrays
        .asList("SuccessfulWorkflowCount", "FailedWorkflowCount", "FailedWorkflowGauge",
            "ExistingWorkflowGauge", "QueuedWorkflowGauge", "RunningWorkflowGauge"));
    for (ObjectInstance i : existingInstances) {
      for (MBeanAttributeInfo info : beanServer.getMBeanInfo(i.getObjectName()).getAttributes()) {
        expectedAttr.remove(info.getName());
      }
    }
    Assert.assertTrue(expectedAttr.isEmpty());

    int successfulWfCnt = 10;
    int failedWfCnt = 10;
    int queuedWfCnt = 10;
    int runningWfCnt = 10;

    for (int i = 0; i < successfulWfCnt; i++) {
      wm.updateWorkflowCounters(TaskState.COMPLETED);
      wm.updateWorkflowGauges(TaskState.COMPLETED);
    }

    for (int i = 0; i < failedWfCnt; i++) {
      wm.updateWorkflowCounters(TaskState.FAILED);
      wm.updateWorkflowGauges(TaskState.FAILED);
    }

    for (int i = 0; i < queuedWfCnt; i++) {
      wm.updateWorkflowGauges(TaskState.NOT_STARTED);
    }

    for (int i = 0; i < runningWfCnt; i++) {
      wm.updateWorkflowGauges(TaskState.IN_PROGRESS);
    }

    // Test gauges
    Assert.assertEquals(wm.getExistingWorkflowGauge(),
        successfulWfCnt + failedWfCnt + queuedWfCnt + runningWfCnt);
    Assert.assertEquals(wm.getFailedWorkflowGauge(), failedWfCnt);
    Assert.assertEquals(wm.getQueuedWorkflowGauge(), queuedWfCnt);
    Assert.assertEquals(wm.getRunningWorkflowGauge(), runningWfCnt);

    // Test counts
    Assert.assertEquals(wm.getFailedWorkflowCount(), failedWfCnt);
    Assert.assertEquals(wm.getSuccessfulWorkflowCount(), successfulWfCnt);

    wm.resetGauges();

    for (int i = 0; i < successfulWfCnt; i++) {
      wm.updateWorkflowCounters(TaskState.COMPLETED);
      wm.updateWorkflowGauges(TaskState.COMPLETED);
    }

    for (int i = 0; i < failedWfCnt; i++) {
      wm.updateWorkflowCounters(TaskState.FAILED);
      wm.updateWorkflowGauges(TaskState.FAILED);
    }

    for (int i = 0; i < queuedWfCnt; i++) {
      wm.updateWorkflowGauges(TaskState.NOT_STARTED);
    }

    for (int i = 0; i < runningWfCnt; i++) {
      wm.updateWorkflowGauges(TaskState.IN_PROGRESS);
    }

    // After reset, counters should be accumulative, but gauges should be reset
    Assert.assertEquals(wm.getExistingWorkflowGauge(),
        successfulWfCnt + failedWfCnt + queuedWfCnt + runningWfCnt);
    Assert.assertEquals(wm.getFailedWorkflowGauge(), failedWfCnt);
    Assert.assertEquals(wm.getQueuedWorkflowGauge(), queuedWfCnt);
    Assert.assertEquals(wm.getRunningWorkflowGauge(), runningWfCnt);
    Assert.assertEquals(wm.getFailedWorkflowCount(), failedWfCnt * 2);
    Assert.assertEquals(wm.getSuccessfulWorkflowCount(), successfulWfCnt * 2);

  }

  private ObjectName getObjectName() throws MalformedObjectNameException {
    return new ObjectName(
        String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), TEST_WORKFLOW_MBEAN_NAME));
  }

  private void registerMbean(Object bean, ObjectName name) {
    try {
      if (beanServer.isRegistered(name)) {
        beanServer.unregisterMBean(name);
      }
    } catch (Exception e) {
      // OK
    }

    try {
      System.out.println("Register MBean: " + name);
      beanServer.registerMBean(bean, name);
    } catch (Exception e) {
      System.out.println("Could not register MBean: " + name + e.toString());
    }
  }

}
