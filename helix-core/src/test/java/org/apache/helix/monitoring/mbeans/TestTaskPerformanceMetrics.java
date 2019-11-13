package org.apache.helix.monitoring.mbeans;

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

import com.google.common.collect.ImmutableMap;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskSynchronizedTestBase;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests that performance profiling metrics via JobMonitorMBean are computed correctly.
 */
public class TestTaskPerformanceMetrics extends TaskSynchronizedTestBase {
  private static final long TASK_LATENCY = 100L;
  // Configurable values for test setup
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();
  private Map<String, Object> _beanValueMap = new HashMap<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  /**
   * Test the following metrics are dynamically emitted:
   * SubmissionToStartDelay
   * ControllerInducedDelay
   * The test schedules a workflow with 30 jobs, each with one task with TASK_LATENCY.
   * AllowOverlapJobAssignment is false, so these jobs will be run in series, one at a time.
   * With this setup, we can assume that the mean value of the metrics above will increase every
   * time we poll at some interval greater than TASK_LATENCY.
   * @throws Exception
   */
  @Test
  public void testTaskPerformanceMetrics() throws Exception {
    // Create a workflow
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
    TaskConfig.Builder taskConfigBuilder = new TaskConfig.Builder();
    List<TaskConfig> taskConfigs = new ArrayList<>();
    TaskConfig taskConfig = taskConfigBuilder.setTaskId("1").setCommand("Reindex").build();
    taskConfig.getConfigMap().put("Latency", Long.toString(TASK_LATENCY));
    taskConfigs.add(taskConfig);
    jobConfigBuilder.addTaskConfigs(taskConfigs)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, Long.toString(TASK_LATENCY)));
    Workflow.Builder workflowBuilder = new Workflow.Builder("wf");
    for (int i = 0; i < 30; i++) {
      workflowBuilder.addJob("job_" + i, jobConfigBuilder);
    }
    Workflow workflow = workflowBuilder.build();

    // Start the controller and start the workflow
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME);
    _controller.syncStart();
    _driver.start(workflow);

    // Confirm that there are metrics computed dynamically here and keeps increasing because jobs
    // are processed one by one
    double oldSubmissionToStartDelay = 0.0d;
    double oldControllerInducedDelay = -1L;

    for (int i = 0; i < 5; i++) {
      // Wait until new dynamic metrics are updated.
      final double oldDelay = oldSubmissionToStartDelay;
      TestHelper.verify(() -> {
        extractMetrics();
        return ((double) _beanValueMap.getOrDefault("SubmissionToScheduleDelayGauge.Mean", 0.0d))
            > oldDelay
            && ((double) _beanValueMap.getOrDefault("SubmissionToProcessDelayGauge.Mean", 0.0d))
            > 0.0d;
      }, TestHelper.WAIT_DURATION);

      // For SubmissionToProcessDelay, the value will stay constant because the Controller will
      // create JobContext right away most of the time
      Assert.assertTrue(_beanValueMap.containsKey("SubmissionToProcessDelayGauge.Mean"));
      Assert.assertTrue(_beanValueMap.containsKey("SubmissionToScheduleDelayGauge.Mean"));
      Assert.assertTrue(_beanValueMap.containsKey("ControllerInducedDelayGauge.Mean"));

      // Get the new values
      double submissionToProcessDelay =
          (double) _beanValueMap.get("SubmissionToProcessDelayGauge.Mean");
      double newSubmissionToScheduleDelay =
          (double) _beanValueMap.get("SubmissionToScheduleDelayGauge.Mean");
      double newControllerInducedDelay =
          (double) _beanValueMap.get("ControllerInducedDelayGauge.Mean");

      Assert.assertTrue(submissionToProcessDelay > 0);
      Assert.assertTrue(oldSubmissionToStartDelay < newSubmissionToScheduleDelay);
      Assert.assertTrue(oldControllerInducedDelay < newControllerInducedDelay);

      oldSubmissionToStartDelay = newSubmissionToScheduleDelay;
      oldControllerInducedDelay = newControllerInducedDelay;
    }
  }

  /**
   * Queries for all MBeans from the MBean Server and only looks at the relevant MBean and gets its
   * metric numbers.
   */
  private void extractMetrics() {
    try {
      QueryExp exp = Query.match(Query.attr("SensorName"), Query.value(CLUSTER_NAME + ".Job.*"));
      Set<ObjectInstance> mbeans = new HashSet<>(
          ManagementFactory.getPlatformMBeanServer().queryMBeans(new ObjectName("ClusterStatus:*"), exp));
      for (ObjectInstance instance : mbeans) {
        ObjectName beanName = instance.getObjectName();
        if (instance.getClassName().endsWith("JobMonitor")) {
          MBeanInfo info = _server.getMBeanInfo(beanName);
          MBeanAttributeInfo[] infos = info.getAttributes();
          for (MBeanAttributeInfo infoItem : infos) {
            Object val = _server.getAttribute(beanName, infoItem.getName());
            _beanValueMap.put(infoItem.getName(), val);
          }
        }
      }
    } catch (Exception e) {
      // update failed
    }
  }
}
