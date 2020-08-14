package org.apache.helix.task;

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

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.model.ClusterConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestElasticScaling extends TaskSynchronizedTestBase {
  private static final String DEFAULT_QUOTA_TYPE = "DEFAULT";

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  private void sleep(int time) {
    long sleepStartTime = System.currentTimeMillis();
    long sleepCurrentTime = sleepStartTime;
    while (sleepCurrentTime - sleepStartTime < time) {
      sleepCurrentTime = System.currentTimeMillis();
    }
  }

  private void monitorZK(HelixManager tmpManager, TaskDriver driver, int testTime) {
    try {
      String traceFilePath = "../demo/elastic-scaling/demo.csv";
      FileWriter file = new FileWriter(traceFilePath, true);
      int time = 0;
      long startTime = System.currentTimeMillis();
      long currentTime = startTime;
      while (currentTime - startTime < testTime) {
        sleep(1000);
        int workflows = driver.getWorkflows().size();
        int liveInstances = tmpManager.getHelixDataAccessor()
            .getChildNames(tmpManager.getHelixDataAccessor().keyBuilder().liveInstances()).size();
        file.write(time + "," + workflows + "," + liveInstances + "\n");
        file.flush();
        ++time;
        currentTime = System.currentTimeMillis();
      }
      file.close();
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage());
    }
  }

  private void submitTestWorkflow(String workflowName, TaskDriver driver, String jobDelay) {
    JobConfig.Builder job = new JobConfig.Builder();
    job.setJobCommandConfigMap(
        Collections.singletonMap(org.apache.helix.integration.task.MockTask.JOB_DELAY, jobDelay));
    Workflow.Builder workflow = new Workflow.Builder(workflowName);
    workflow.setExpiry(1);
    job.setWorkflow(workflowName);
    TaskConfig taskConfig =
        new TaskConfig(MockTask.TASK_COMMAND, new HashMap<String, String>(), null, null);
    job.addTaskConfigMap(Collections.singletonMap(taskConfig.getId(), taskConfig));
    job.setJobId(TaskUtil.getNamespacedJobName(workflowName, "JOB"));
    workflow.addJob("JOB", job);
    driver.start(workflow.build());
  }

  @Test
  public void testMockParticipantTaskRegistration() throws Exception {
    String clusterName = "MYCLUSTER";
    String zkAddr = "172.17.0.3:30100";
    String taskCommand = "Reindex";
    String taskJarPath = "src/test/resources/Reindex.jar";
    String taskVersion = "1.0.0";
    String fullyQualifiedTaskClassName = "com.mycompany.mocktask.MockTask";
    String fullyQualifiedTaskFactoryClassName = "com.mycompany.mocktask.MockTaskFactory";

    HelixManager tmpManager = HelixManagerFactory.getZKHelixManager(clusterName, "Admin", InstanceType.ADMINISTRATOR, zkAddr);
    tmpManager.connect();

    // Add task definition information as a DynamicTaskConfig.
    List<String> taskClasses = new ArrayList();
    taskClasses.add(fullyQualifiedTaskClassName);
    DynamicTaskConfig taskConfig = new DynamicTaskConfig(taskCommand, taskJarPath, taskVersion, taskClasses, fullyQualifiedTaskFactoryClassName);
    String path = TaskConstants.DYNAMICALLY_LOADED_TASK_PATH + "/" + taskCommand;
    tmpManager.getHelixDataAccessor().getBaseDataAccessor().create(path, taskConfig.getTaskConfigZNRecord(), AccessOption.PERSISTENT);

    // Set Default quota to 1 to easier see autoscaling happening
    ClusterConfig clusterConfig = tmpManager.getConfigAccessor().getClusterConfig(clusterName);
    clusterConfig.resetTaskQuotaRatioMap();
    clusterConfig.setTaskQuotaRatio(DEFAULT_QUOTA_TYPE,1);
    clusterConfig.setTaskQuotaRatio("A", 39);

    // Launch another thread to monitor the cluster
    tmpManager.getConfigAccessor().setClusterConfig(clusterName, clusterConfig);
    TaskDriver driver = new TaskDriver(tmpManager);
    ExecutorService service = Executors.newFixedThreadPool(1);
    CountDownLatch latch = new CountDownLatch(1);
    service.execute(() -> {
      monitorZK(tmpManager, driver, 200000);
      latch.countDown();
    });

    // Submit workflows for the demo
    sleep(1000);
    for (int i = 0; i < 15; i++) {
      submitTestWorkflow("Workflow" + i, driver, "15000");
    }
    sleep(80000);
    for (int i = 15; i < 30; i++) {
      submitTestWorkflow("Workflow" + i, driver, "15000");
    }
    sleep(80000);
    latch.await();
    tmpManager.disconnect();
  }
}
