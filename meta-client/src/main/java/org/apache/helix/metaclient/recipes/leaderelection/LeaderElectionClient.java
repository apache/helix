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
 * The Leader Election client maintains and elect an active leader from participant pool.
 * All participants wanted to be elected as leader joins a pool.
 * LeaderElection client maintains an active leader, by monitoring liveness of current leader and
 * re-elect if needed and user no need to call elect or re-elect explicitly.
 * This LeaderElection client will notify registered listeners for any leadership change.
 *
 * One client is created per each participant(host). One participant can join multiple leader
 * election groups using the same client.
 * When the client is used by a leader election service, one client is created for each participant.
 *
 */
public class LeaderElectionClient {

  /**
   * Construct a LeaderElectionClient using a user passed in leaderElectionConfig. It creates a MetaClient
   * instance underneath.
   * When MetaClient is auto closed be cause of being disconnected and auto retry connection timed out, A new
   * MetaClient instance will be created and keeps retry connection.
   *
   * @param metaClientConfig The config used to create an metaclient.
   */
  public LeaderElectionClient(MetaClientConfig metaClientConfig, String participant) {

  }

  /**
   * Construct a LeaderElectionClient using a user passed in MetaClient object
   * When MetaClient is auto closed be cause of being disconnected and auto retry connection timed out, user
   * will need to create a new MetaClient and a new LeaderElectionClient instance.
   *
   * @param metaClient metaClient object to be used.
   */
  public LeaderElectionClient(MetaClientInterface metaClient, String participant) {

  }

  /**
   * Returns true if current participant is the current leadership.
   */
  public boolean isLeader(String leaderPath) {
    return false;
  }

  /**
   * Participants join a leader election group by calling the following API.
   * The Leader Election client maintains and elect an active leader from the participant pool.
   *
   * @param leaderPath The path for leader election.
   * @return boolean indicating if the operation is succeeded.
   */
  public boolean joinLeaderElectionParticipantPool(String leaderPath) {
    return false;
  }

  /**
   * Participants join a leader election group by calling the following API.
   * The Leader Election client maintains and elect an active leader from the participant pool.
   *
   * @param leaderPath The path for leader election.
   * @param userInfo Any additional information to associate with this participant.
   * @return boolean indicating if the operation is succeeded.
   */
  public boolean joinLeaderElectionParticipantPool(String leaderPath, Object userInfo) {
    return false;
  }

  /**
   * Any participant may exit the exitLeaderElectionParticipantPool by calling the API.
   * If the participant is not the current leader, it leaves the pool and won't participant future
   * leader election process.
   * If the participant is the current leader, it leaves the pool and a new leader will be elected
   * if there are other participants in the pool.
   * Throws exception if the participant is not in the pool.
   *
   * @param leaderPath The path for leader election.
   * @return boolean indicating if the operation is succeeded.
   *
   * @throws RuntimeException If the participant did not join participant pool via this client. // TODO: define exp type
   */
  public boolean exitLeaderElectionParticipantPool(String leaderPath) {
    return false;
  }

  /**
   * Releases leadership for participant.
   *
   * @param leaderPath The path for leader election.
   *
   * @throws RuntimeException if the leadership is not owned by this participant, or if the
   *                          participant did not join participant pool via this client. // TODO: define exp type
   */
  public void relinquishLeader(String leaderPath) {
  }

  /**
   * Get current leader.
   *
   * @param leaderPath The path for leader election.
   * @return Returns the current leader. Return null if no Leader at a given point.
   * @throws RuntimeException when leader path does not exist. // TODO: define exp type
   */
  public String getLeader(String leaderPath) {
    return null;
  }

  /**
   * Return a list of hosts in participant pool
   *
   * @param leaderPath The path for leader election.
   * @return a list of participant(s) that tried to elect themselves as leader. The current leader
   *         is not included in the list.
   *         Return an empty list if
   *          1. There is a leader for this path but there is no other participants
   *          2. There is no leader for this path at the time of query
   * @throws RuntimeException when leader path does not exist. // TODO: define exp type
   */
  public List<String> getParticipants(String leaderPath) {
    return null;
  }

  /**
   * APIs to register/unregister listener to leader path. All the participants can listen to any
   * leaderPath, Including leader going down or a new leader comes up.
   * Whenever current leader for that leaderPath goes down (considering it's ephemeral entity which
   * get's auto-deleted after TTL or session timeout) or a new leader comes up, it notifies all
   * participants who have been listening on entryChange event.
   *
   * An listener will still be installed if the path does not exists yet.
   *
   * @param leaderPath The path for leader election that listener is interested for change.
   * @param listener An implementation of LeaderElectionListenerInterface
   * @return an boolean value indicating if registration is success.
   */
  public boolean subscribeLeadershipChanges(String leaderPath,
      LeaderElectionListenerInterface listener) {
    return false;
  }

  /**
   * @param leaderPath The path for leader election that listener is no longer interested for change.
   * @param listener An implementation of LeaderElectionListenerInterface
   */
  public void unsubscribeLeadershipChanges(String leaderPath,
      LeaderElectionListenerInterface listener) {
  }
}

