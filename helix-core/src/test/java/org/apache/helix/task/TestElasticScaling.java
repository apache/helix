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
    while(sleepCurrentTime - sleepStartTime < time) {
      sleepCurrentTime = System.currentTimeMillis();
    }
  }
  private void monitorZK(HelixManager tmpManager, TaskDriver driver, int testTime) {
    try {
      FileWriter file = new FileWriter("../demo/elastic-scaling/demo.csv", true);
      int time = 0;
      long startTime = System.currentTimeMillis();
      long currentTime = startTime;
      while(currentTime - startTime < testTime) {
        sleep(1000);
        int workflows = driver.getWorkflows().size();
        int liveInstances = tmpManager.getHelixDataAccessor().getChildNames(tmpManager.getHelixDataAccessor().keyBuilder().liveInstances()).size();
        file.write(time + "," + workflows + "," + liveInstances + "\n");
        file.flush();
        ++time;
        currentTime = System.currentTimeMillis();
      }
      file.close();
    } catch (IOException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
    }
  }

  private void submitTestWorkflow(String workflowName, TaskDriver driver, String jobDelay) {
    JobConfig.Builder job = new JobConfig.Builder();
    job.setJobCommandConfigMap(Collections.singletonMap(org.apache.helix.integration.task.MockTask.JOB_DELAY, jobDelay));
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
    HelixManager tmpManager = HelixManagerFactory.getZKHelixManager("MYCLUSTER", "Admin",
        InstanceType.ADMINISTRATOR, "172.17.0.3:30100");
    tmpManager.connect();

    // Add task definition information as a DynamicTaskConfig.
    List<String> taskClasses = new ArrayList();
    taskClasses.add("com.mycompany.mocktask.MockTask");
    DynamicTaskConfig taskConfig =
        new DynamicTaskConfig("Reindex", "src/test/resources/Reindex.jar", "1.0.0", taskClasses,
            "com.mycompany.mocktask.MockTaskFactory");
    String path = TaskConstants.DYNAMICALLY_LOADED_TASK_PATH + "/Reindex";
    tmpManager.getHelixDataAccessor().getBaseDataAccessor()
        .create(path, taskConfig.getTaskConfigZNRecord(), AccessOption.PERSISTENT);

    ClusterConfig clusterConfig = tmpManager.getConfigAccessor().getClusterConfig("MYCLUSTER"); // Retrieve ClusterConfig
    clusterConfig.resetTaskQuotaRatioMap(); // Optional: you may want to reset the quota config before creating a new quota config
    clusterConfig.setTaskQuotaRatio(DEFAULT_QUOTA_TYPE, 1); // Define the default quota (DEFAULT_QUOTA_TYPE = "DEFAULT")
    clusterConfig.setTaskQuotaRatio("A", 39); // Define quota type A
    tmpManager.getConfigAccessor().setClusterConfig("MYCLUSTER", clusterConfig); // Set the new ClusterConfig
    TaskDriver driver = new TaskDriver(tmpManager);
    ExecutorService service = Executors.newFixedThreadPool(1);
    CountDownLatch latch = new CountDownLatch(1);
    service.execute(() -> {
      monitorZK(tmpManager, driver, 200000);
      latch.countDown();
    });
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
