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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.task.RuntimeJobDag;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRuntimeJobDag {
  private Set<String> actualJobs;
  private Set<String> expectedJobs;

  @Test
  public void testQueue() {
    // Test a queue: 1->2->3->4, expect [4, 3, 2, 1]
    List<String> jobs_1 = new ArrayList<>();
    jobs_1.add("4");
    jobs_1.add("3");
    jobs_1.add("2");
    jobs_1.add("1");
    RuntimeJobDag jobDag_1 = createJobDag(jobs_1);
    jobDag_1.addParentToChild("4", "3");
    jobDag_1.addParentToChild("3", "2");
    jobDag_1.addParentToChild("2", "1");
    jobDag_1.validate();
    jobDag_1.generateJobList();
    Assert.assertTrue(jobDag_1.hasNextJob());
    Assert.assertEquals(jobDag_1.getNextJob(), "4");
    jobDag_1.finishJob("4");
    Assert.assertEquals(jobDag_1.getNextJob(), "3");
    jobDag_1.finishJob("3");
    Assert.assertEquals(jobDag_1.getNextJob(), "2");
    jobDag_1.finishJob("2");
    Assert.assertEquals(jobDag_1.getNextJob(), "1");
    jobDag_1.finishJob("1");
    Assert.assertFalse(jobDag_1.hasNextJob());
  }

  @Test(dependsOnMethods = "testQueue")
  public void testAddAndRemove() {
    // Test a queue: 1->2->3->4, expect [4, 3, 2, 1]
    List<String> jobs_1 = new ArrayList<>();
    jobs_1.add("4");
    jobs_1.add("3");
    jobs_1.add("2");
    jobs_1.add("1");
    RuntimeJobDag jobDag_1 = createJobDag(jobs_1);
    jobDag_1.addParentToChild("4", "3");
    jobDag_1.addParentToChild("3", "2");
    jobDag_1.addParentToChild("2", "1");
    jobDag_1.validate();
    jobDag_1.generateJobList();
    Assert.assertEquals(jobDag_1.getNextJob(), "4");
    jobDag_1.finishJob("4");
    jobDag_1.addNode("0");
    jobDag_1.addParentToChild("0", "4");
    Assert.assertEquals(jobDag_1.getNextJob(), "0"); // Should automatically re-generate job list
    jobDag_1.removeNode("3", true);
    jobDag_1.removeNode("0", true);
    Assert.assertEquals(jobDag_1.getNextJob(), "4"); // Should automatically re-generate job list and start over
    jobDag_1.finishJob("4");
    Assert.assertEquals(jobDag_1.getNextJob(), "2"); // Should skip 3
  }

  @Test(dependsOnMethods = "testAddAndRemove")
  public void testRegularDAGAndGenerateJobList() {
    List<String> jobs = new ArrayList<>();
    // DAG from
    // https://en.wikipedia.org/wiki/Topological_sorting#/media/File:Directed_acyclic_graph_2.svg
    jobs.add("5");
    jobs.add("11");
    jobs.add("2");
    jobs.add("7");
    jobs.add("8");
    jobs.add("9");
    jobs.add("3");
    jobs.add("10");
    RuntimeJobDag jobDag = createJobDag(jobs);
    jobDag.addParentToChild("5", "11");
    jobDag.addParentToChild("11", "2");
    jobDag.addParentToChild("7", "11");
    jobDag.addParentToChild("7", "8");
    jobDag.addParentToChild("11", "9");
    jobDag.addParentToChild("11", "10");
    jobDag.addParentToChild("8", "9");
    jobDag.addParentToChild("3", "8");
    jobDag.addParentToChild("3", "10");
    jobDag.generateJobList();

    testRegularDAGHelper(jobDag);
    jobDag.generateJobList();
    // Should have the original job list
    testRegularDAGHelper(jobDag);
  }

  private void testRegularDAGHelper(RuntimeJobDag jobDag) {
    emptyJobSets();
    // 5, 7, 3 are un-parented nodes to start with
    actualJobs.add(jobDag.getNextJob());
    actualJobs.add(jobDag.getNextJob());
    actualJobs.add(jobDag.getNextJob());
    Assert.assertFalse(jobDag.hasNextJob());
    expectedJobs.add("5");
    expectedJobs.add("7");
    expectedJobs.add("3");
    Assert.assertEquals(actualJobs, expectedJobs);
    jobDag.finishJob("3");

    // Once 3 finishes, ready-list should still be empty
    Assert.assertFalse(jobDag.hasNextJob());

    jobDag.finishJob("7");
    // Once 3 and 7 both finish, 8 should be ready
    emptyJobSets();
    actualJobs.add(jobDag.getNextJob());
    expectedJobs.add("8");
    Assert.assertEquals(actualJobs, expectedJobs);
    Assert.assertFalse(jobDag.hasNextJob());

    jobDag.finishJob("5");
    // Once 5 finishes, 11 should be ready
    emptyJobSets();
    expectedJobs.add(jobDag.getNextJob());
    actualJobs.add("11");
    Assert.assertEquals(actualJobs, expectedJobs);
    Assert.assertFalse(jobDag.hasNextJob());

    jobDag.finishJob("11");
    jobDag.finishJob("8");
    // Once 11 and 8 finish, 2, 9, 10 should be ready
    emptyJobSets();
    actualJobs.add(jobDag.getNextJob());
    actualJobs.add(jobDag.getNextJob());
    actualJobs.add(jobDag.getNextJob());
    expectedJobs.add("2");
    expectedJobs.add("10");
    expectedJobs.add("9");
    Assert.assertEquals(actualJobs, expectedJobs);
    Assert.assertFalse(jobDag.hasNextJob());

    // When all jobs are finished, no jobs should be left
    jobDag.finishJob("9");
    jobDag.finishJob("10");
    jobDag.finishJob("2");
    Assert.assertFalse(jobDag.hasNextJob());
  }

  private RuntimeJobDag createJobDag(List<String> jobs) {
    RuntimeJobDag jobDag = new RuntimeJobDag();
    for (String job : jobs) {
      jobDag.addNode(job);
    }
    return jobDag;
  }

  private void emptyJobSets() {
    actualJobs = new HashSet<>();
    expectedJobs = new HashSet<>();
  }
}