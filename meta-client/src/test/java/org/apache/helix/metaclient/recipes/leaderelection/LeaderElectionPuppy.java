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

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.helix.metaclient.MetaClientTestUtil;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.puppy.AbstractPuppy;
import org.apache.helix.metaclient.puppy.PuppySpec;
import org.testng.Assert;


public class LeaderElectionPuppy extends AbstractPuppy {
  String _leaderGroup;
  String _participant;
  private final Random _random;
  LeaderElectionClient _leaderElectionClient;

  public LeaderElectionPuppy(LeaderElectionClient leaderElectionClient, PuppySpec puppySpec, String leaderGroup,
      String participant) {
    super(leaderElectionClient.getMetaClient(), puppySpec, leaderGroup);
    _leaderElectionClient = leaderElectionClient;
    _leaderGroup = leaderGroup;
    _random = new Random();
    _participant = participant;
  }

  @Override
  protected void bark() throws Exception {
    int randomNumber = _random.nextInt((int) TimeUnit.SECONDS.toMillis(5));
    System.out.println("LeaderElectionPuppy " + _participant + " Joining");
    _leaderElectionClient.joinLeaderElectionParticipantPool(_leaderGroup);

    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (_leaderElectionClient.getLeader(_leaderGroup) != null);
    }, MetaClientTestUtil.WAIT_DURATION));
    if (_random.nextBoolean()) {
      _leaderElectionClient.relinquishLeader(_leaderGroup);
    }
    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (_leaderElectionClient.getParticipantInfo(_leaderGroup, _participant) != null);
    }, MetaClientTestUtil.WAIT_DURATION));

    Thread.sleep(randomNumber);
    System.out.println("LeaderElectionPuppy " +  _participant + " Leaving");
    _leaderElectionClient.exitLeaderElectionParticipantPool(_leaderGroup);
    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (_leaderElectionClient.getParticipantInfo(_leaderGroup, _participant) == null);
    }, MetaClientTestUtil.WAIT_DURATION));

    Thread.sleep(randomNumber);
  }

  @Override
  protected void cleanup() {
    try {
      System.out.println("Cleaning - LeaderElectionPuppy " +  _participant + " Leaving");
      _leaderElectionClient.exitLeaderElectionParticipantPool(_leaderGroup);
    } catch (MetaClientException ignore) {
      // already leave the pool. OK to throw exception.
    } finally {
      try {
        _leaderElectionClient.close();
      } catch (Exception e) {
      }
    }
  }
}
