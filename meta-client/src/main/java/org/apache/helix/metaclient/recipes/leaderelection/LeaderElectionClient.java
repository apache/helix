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

import java.util.List;

import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.factories.MetaClientConfig;


/**
 * LeaderElectionClient does distributed leader election using CRUD and change notification APIs
 * provided by underlying metadata client. Leader election config can provide many
 * configs like base path for all participating nodes, sync/async mode, TTL etc.
 *
 * Participants join a leader election group by calling the following API.
 * The Leader Election client maintains and elect an active leader from participant pool
 * All participants wanted to be elected as leader joins a pool
 * LeaderElection client maintains an active leader, by monitoring liveness of current leader and
 * re-elect if needed and user no need to call elect or re-elect explicitly.
 * This LeaderElection client will notify registered listeners for any leadership change.
 */
public class LeaderElectionClient {

  /**
   * Construct a LeaderElectionClient using a user passed in leaderElectionConfig. It creates a MetaClient
   * instance underneath.
   * When MetaClient is auto closed be cause of being disconnected and auto retry connection timed out, A new
   * MetaClient instance will be created and keeps retry connection.
   */
  public LeaderElectionClient(MetaClientConfig metaClientConfig) {

  }

  /**
   * Construct a LeaderElectionClient using a user passed in MetaClient object
   * When MetaClient is auto closed be cause of being disconnected and auto retry connection timed out, user
   * will need to create a new MetaClient and a new LeaderElectionClient instance.
   */
  public LeaderElectionClient(MetaClientInterface MetaClient) {

  }

  /**
   * Returns true if current participant is able to acquire leadership.
   */
  public boolean isLeader(String leaderPath, String participant) {
    return false;
  }

  /**
   * Participants join a leader election group by calling the following API.
   * The Leader Election client maintains and elect an active leader from participant pool
   */
  public boolean joinLeaderElectionParticipantPool(String leaderPath, String participant,
      Object userInfo) {
    return false;
  }

  /**
   * Any participant may exit the exitLeaderElectionParticipantPool by calling the API.
   * If the participant is not the current leader, it leaves the pool and won't be elected as next leader.
   * If the participant is the current leader, it leaves the pool and a new leader will be elected if there are other participants in the pool.
   * Throws exception if the participant is not in the pool.
   */
  public boolean exitLeaderElectionParticipantPool(String leaderPath, String participant) {
    return false;
  }

  /**
   * Releases leadership for participant.
   * Throws exception if the leadership is not owned by this participant.
   */
  public void relinquishLeader(String leaderPath, String participant) {
  }

  /**
   * Get current leader nodes.
   * Returns null if no Leader at a given point.
   */
  public String getLeader(String leaderPath) {
    return null;
  }

  /**
   * Return a list of hosts in participant pool
   */
  public List<String> getFollowers(String leaderPath) {
    return null;
  }

  /**
   * APIs to register/unregister listener to leader path. All the participants can listen to any
   * leaderPath, Including leader going down or a new leader comes up.
   * Whenever current leader for that leaderPath goes down (considering it's ephemeral entity which
   * get's auto-deleted after TTL or session timeout) or a new leader comes up, it notifies all
   * participants who have been listening on entryChange event.
   */
  public boolean subscribeLeadershipChanges(String leaderPath,
      LeaderElectionListenerInterface listener) {
    return false;
  }

  public void unsubscribeLeadershipChanges(String leaderPath,
      LeaderElectionListenerInterface listener) {
  }
}

