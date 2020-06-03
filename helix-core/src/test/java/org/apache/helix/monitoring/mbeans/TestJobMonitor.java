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

import org.apache.helix.AccessOption;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.task.assigner.AssignableInstance;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestJobMonitor extends ZkTestBase {
  private static final String TEST_JOB_TYPE = AssignableInstance.DEFAULT_QUOTA_TYPE;

  private JobMonitor _monitor;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    ClusterStatusMonitor clusterStatusMonitor = new ClusterStatusMonitor("testClusterName");
    _monitor = new JobMonitor("testClusterName", TEST_JOB_TYPE,
        clusterStatusMonitor.getObjectName(clusterStatusMonitor.getJobBeanName(TEST_JOB_TYPE)));
    _monitor.register();
  }

  @AfterClass
  public void afterClass() throws Exception {
    _monitor.unregister();
  }

  @Test()
  public void testSyncSubmissionToProcessDelayToZk() {
    _monitor.updateSubmissionToProcessDelayGauge(100L);
    _monitor.syncSubmissionToProcessDelayToZk(_baseAccessor);

    ZNRecord znRecord = _baseAccessor
        .get(JobMonitor.buildZkPathForJobMonitorMetric(TEST_JOB_TYPE), new Stat(),
            AccessOption.PERSISTENT);
    // Cannot verify the value because the metric may be invoked by other tests
    Assert.assertNotNull(znRecord
        .getSimpleField(JobMonitor.JobMonitorMetricZnodeField.SUBMISSION_TO_PROCESS_DELAY.name()));
  }

  @Test(dependsOnMethods = "testSyncSubmissionToProcessDelayToZk")
  public void testSyncSubmissionToScheduleDelayToZk() {
    _monitor.updateSubmissionToScheduleDelayGauge(100L);
    _monitor.syncSubmissionToScheduleDelayToZk(_baseAccessor);

    ZNRecord znRecord = _baseAccessor
        .get(JobMonitor.buildZkPathForJobMonitorMetric(TEST_JOB_TYPE), new Stat(),
            AccessOption.PERSISTENT);
    // Cannot verify the value because the metric may be invoked by other tests
    Assert.assertNotNull(znRecord
        .getSimpleField(JobMonitor.JobMonitorMetricZnodeField.SUBMISSION_TO_SCHEDULE_DELAY.name()));
  }

  @Test(dependsOnMethods = "testSyncSubmissionToScheduleDelayToZk")
  public void testSyncControllerInducedDelayToZk() {
    _monitor.updateControllerInducedDelayGauge(100L);
    _monitor.syncControllerInducedDelayToZk(_baseAccessor);

    ZNRecord znRecord = _baseAccessor
        .get(JobMonitor.buildZkPathForJobMonitorMetric(TEST_JOB_TYPE), new Stat(),
            AccessOption.PERSISTENT);
    // Cannot verify the value because the metric may be invoked by other tests
    Assert.assertNotNull(znRecord.getSimpleField(
        JobMonitor.JobMonitorMetricZnodeField.CONTROLLER_INDUCED_PROCESS_DELAY.name()));
  }
}
