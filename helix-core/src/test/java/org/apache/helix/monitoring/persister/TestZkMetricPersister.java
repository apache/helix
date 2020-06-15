package org.apache.helix.monitoring.persister;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.AccessOption;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.JobMonitor;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.metrics.model.Metric;
import org.apache.helix.task.assigner.AssignableInstance;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZkMetricPersister extends ZkTestBase {
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
  public void testPersistMetricsMeanValues() throws Exception {
    String clusterName = "testClusterName";
    ClusterStatusMonitor clusterStatusMonitor = new ClusterStatusMonitor(clusterName);
    MetricPersister metricPersister = new ZkMetricPersister(_baseAccessor);

    JobMonitor jobMonitor = new JobMonitor("testClusterName", TEST_JOB_TYPE,
        clusterStatusMonitor.getObjectName(clusterStatusMonitor.getJobBeanName(TEST_JOB_TYPE)));
    jobMonitor.updateSubmissionToProcessDelayGauge(100L);
    jobMonitor.updateSubmissionToScheduleDelayGauge(100L);
    jobMonitor.updateControllerInducedDelayGauge(100L);

    Set<HistogramDynamicMetric> metricsToPersist = new HashSet<>();
    metricsToPersist.add(jobMonitor.getSubmissionToProcessDelayGauge());
    metricsToPersist.add(jobMonitor.getSubmissionToScheduleDelayGauge());
    metricsToPersist.add(jobMonitor.getControllerInducedDelayGauge());

    Map<String, String> persisterMetadata = new HashMap<>();
    persisterMetadata.put(ZkMetricPersister.ZkMetricPersisterMetadataKey.ZNODE_NAME.name(), jobMonitor.getJobType());
    persisterMetadata.put(ZkMetricPersister.ZkMetricPersisterMetadataKey.CLUSTER_NAME.name(), clusterName);

    metricPersister.persistMetricsMeanValues(metricsToPersist, persisterMetadata);

    ZNRecord znRecord = _baseAccessor
        .get(ZkMetricPersister.buildMetricPersistZkPath(clusterName, TEST_JOB_TYPE), new Stat(),
            AccessOption.PERSISTENT);
    // Cannot verify the value because the metric may be invoked by other tests
    Assert.assertNotNull(znRecord.getSimpleField(JobMonitor.SUBMISSION_TO_PROCESS_DELAY_GAUGE_NAME));
    Assert.assertNotNull(znRecord.getSimpleField(JobMonitor.SUBMISSION_TO_SCHEDULE_DELAY_GAUGE_NAME));
    Assert.assertNotNull(znRecord.getSimpleField(JobMonitor.CONTROLLER_INDUCED_DELAY_GAUGE_NAME));
  }
}
