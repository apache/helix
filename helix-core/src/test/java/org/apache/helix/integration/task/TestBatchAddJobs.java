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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestBatchAddJobs extends ZkTestBase {
  private static final String CLUSTER_NAME = CLUSTER_PREFIX + "_TestBatchAddJobs";
  private static final String QUEUE_NAME = "TestBatchAddJobQueue";
  private ClusterSetup _setupTool;
  private List<SubmitJobTask> _submitJobTasks;

  @BeforeClass
  public void beforeClass() {
    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    _submitJobTasks = new ArrayList<>();
  }

  @Test
  public void testBatchAddJobs() throws Exception {
    TaskDriver driver = new TaskDriver(_gZkClient, CLUSTER_NAME);
    driver.createQueue(new JobQueue.Builder(QUEUE_NAME).build());
    for (int i = 0; i < 10; i++) {
      _submitJobTasks.add(new SubmitJobTask(ZK_ADDR, i));
      _submitJobTasks.get(i).start();
    }

    WorkflowConfig workflowConfig = driver.getWorkflowConfig(QUEUE_NAME);
    while (workflowConfig.getJobDag().getAllNodes().size() < 100) {
      Thread.sleep(50);
      driver.getWorkflowConfig(QUEUE_NAME);
    }

    JobDag dag = workflowConfig.getJobDag();
    String currentJob = dag.getAllNodes().iterator().next();
    while (dag.getDirectChildren(currentJob).size() > 0) {
      String childJob = dag.getDirectChildren(currentJob).iterator().next();
      if (!getPrefix(currentJob).equals(getPrefix(childJob))
          && currentJob.charAt(currentJob.length() - 1) != '9') {
        Assert.fail();
      }
      currentJob = childJob;
    }
  }

  private String getPrefix(String job) {
    return job.split("#")[0];
  }

  @AfterClass
  public void afterClass() {
    for (SubmitJobTask submitJobTask : _submitJobTasks) {
      submitJobTask.interrupt();
    }

    deleteCluster(CLUSTER_NAME);
  }

  static class SubmitJobTask extends Thread {
    private TaskDriver _driver;
    private String _jobPrefixName;

    public SubmitJobTask(String zkAddress, int index) throws Exception {
      HelixManager manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Administrator",
          InstanceType.ADMINISTRATOR, zkAddress);
      manager.connect();
      _driver = new TaskDriver(manager);
      _jobPrefixName = "JOB_" + index + "#";
    }

    @Override
    public void start() {
      List<String> jobNames = new ArrayList<>();
      List<JobConfig.Builder> jobConfigBuilders = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        String jobName = _jobPrefixName + i;
        jobNames.add(jobName);
        jobConfigBuilders.add(new JobConfig.Builder().addTaskConfigs(Collections
            .singletonList(new TaskConfig("CMD", null, UUID.randomUUID().toString(), "TARGET"))));
      }

      _driver.enqueueJobs(QUEUE_NAME, jobNames, jobConfigBuilders);
    }
  }
}
