package org.apache.helix.metaclient.recipes.leaderelection;

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

import org.apache.helix.metaclient.MetaClientTestUtil;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest.CreatePuppy;
import org.apache.helix.metaclient.impl.zk.ZkMetaClient;
import org.apache.helix.metaclient.impl.zk.ZkMetaClientTestBase;
import org.apache.helix.metaclient.puppy.ExecDelay;
import org.apache.helix.metaclient.puppy.PuppyManager;
import org.apache.helix.metaclient.puppy.PuppyMode;
import org.apache.helix.metaclient.puppy.PuppySpec;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.helix.metaclient.impl.zk.ZkMetaClientTestBase.*;


public class TestMultiClientLeaderElection extends ZkMetaClientTestBase {
  private final String _leaderElectionGroup = "/Parent/a/LEADER_ELECTION_GROUP";
  private ZkMetaClient<String> _zkMetaClient;
  private final String _participant1 = "participant_1";
  private final String _participant2 = "participant_2";

  @BeforeTest
  private void setUp() {
    System.out.println("STARTING TestMultiClientLeaderElection");
    this._zkMetaClient = createZkMetaClient();
    this._zkMetaClient.connect();
    _zkMetaClient.create("/Parent", "");
    _zkMetaClient.create("/Parent/a", "");
  }
  @AfterTest
  public void cleanUp() {
    try {
      _zkMetaClient.recursiveDelete(_leaderElectionGroup);
    } catch (MetaClientException ex) {
      _zkMetaClient.recursiveDelete(_leaderElectionGroup);
    }
  }

  @Test
  public void testLeaderElectionPuppy() {
    System.out.println("Starting TestMultiClientLeaderElection.testLeaderElectionPuppy");
    PuppySpec puppySpec =
        new org.apache.helix.metaclient.puppy.PuppySpec(PuppyMode.REPEAT, 0.2f, new ExecDelay(5000, 0.1f), 5);
    LeaderElectionPuppy leaderElectionPuppy1 =
        new LeaderElectionPuppy(TestLeaderElection.createLeaderElectionClient(_participant1),
            puppySpec, _leaderElectionGroup, _participant1);
    LeaderElectionPuppy leaderElectionPuppy2 =
        new LeaderElectionPuppy(TestLeaderElection.createLeaderElectionClient(_participant2),
            puppySpec, _leaderElectionGroup, _participant2);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(leaderElectionPuppy1);
    puppyManager.addPuppy(leaderElectionPuppy2);

    puppyManager.start(60);
    System.out.println("Ending TestMultiClientLeaderElection.testLeaderElectionPuppy");

  }
}
