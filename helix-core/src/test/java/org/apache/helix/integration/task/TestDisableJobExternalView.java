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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.model.ExternalView;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestDisableJobExternalView extends TaskTestBase {
  private static final Logger LOG = Logger.getLogger(TestDisableJobExternalView.class);

  @Test
  public void testJobsDisableExternalView() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    ExternviewChecker externviewChecker = new ExternviewChecker();
    _manager.addExternalViewChangeListener(externviewChecker);

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName);

    JobConfig.Builder job1 = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
        .setTargetPartitionStates(Sets.newHashSet("SLAVE"));

    JobConfig.Builder job2 = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
        .setTargetPartitionStates(Sets.newHashSet("SLAVE")).setDisableExternalView(true);

    JobConfig.Builder job3 = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
        .setTargetPartitionStates(Sets.newHashSet("MASTER")).setDisableExternalView(false);

    // enqueue jobs
    queueBuilder.enqueueJob("job1", job1);
    queueBuilder.enqueueJob("job2", job2);
    queueBuilder.enqueueJob("job3", job3);

    _driver.createQueue(queueBuilder.build());

    // ensure all jobs are completed
    String namedSpaceJob3 = String.format("%s_%s", queueName, "job3");
    _driver.pollForJobState(queueName, namedSpaceJob3, TaskState.COMPLETED);

    Set<String> seenExternalViews = externviewChecker.getSeenExternalViews();
    String namedSpaceJob1 = String.format("%s_%s", queueName, "job1");
    String namedSpaceJob2 = String.format("%s_%s", queueName, "job2");

    Assert.assertTrue(seenExternalViews.contains(namedSpaceJob1),
        "Can not find external View for " + namedSpaceJob1 + "!");
    Assert.assertTrue(!seenExternalViews.contains(namedSpaceJob2),
        "External View for " + namedSpaceJob2 + " shoudld not exist!");
    Assert.assertTrue(seenExternalViews.contains(namedSpaceJob3),
        "Can not find external View for " + namedSpaceJob3 + "!");

    _manager
        .removeListener(new PropertyKey.Builder(CLUSTER_NAME).externalViews(), externviewChecker);
  }

  private static class ExternviewChecker implements ExternalViewChangeListener {
    private Set<String> _seenExternalViews = new HashSet<String>();

    @Override public void onExternalViewChange(List<ExternalView> externalViewList,
        NotificationContext changeContext) {
      for (ExternalView view : externalViewList) {
        _seenExternalViews.add(view.getResourceName());
      }
    }

    public Set<String> getSeenExternalViews() {
      return _seenExternalViews;
    }
  }
}

