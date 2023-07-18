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

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;

import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.api.Op;
import org.apache.helix.metaclient.api.OpResult;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.exception.MetaClientNoNodeException;
import org.apache.helix.metaclient.exception.MetaClientNodeExistsException;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.metaclient.api.OpResult.Type.*;


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
public class LeaderElectionClient implements AutoCloseable {

  private final MetaClientInterface<LeaderInfo> _metaClient;
  private final String _participant;
  private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionClient.class);

  // A list of leader election group that this client joins.
  private Set<String> _leaderGroups = new HashSet<>();

  private final static String LEADER_ENTRY_KEY = "/LEADER";
  ReElectListener _reElectListener = new ReElectListener();

  /**
   * Construct a LeaderElectionClient using a user passed in leaderElectionConfig. It creates a MetaClient
   * instance underneath.
   * When MetaClient is auto closed be cause of being disconnected and auto retry connection timed out, A new
   * MetaClient instance will be created and keeps retry connection.
   *
   * @param metaClientConfig The config used to create an metaclient.
   */
  public LeaderElectionClient(MetaClientConfig metaClientConfig, String participant) {
    _participant = participant;
    if (metaClientConfig == null) {
      throw new IllegalArgumentException("MetaClientConfig cannot be null.");
    }
    LOG.info("Creating MetaClient for LeaderElectionClient");
    if (MetaClientConfig.StoreType.ZOOKEEPER.equals(metaClientConfig.getStoreType())) {
      ZkMetaClientConfig zkMetaClientConfig = new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(
          metaClientConfig.getConnectionAddress()).setZkSerializer((new LeaderInfoSerializer())).build();
      _metaClient = new ZkMetaClientFactory().getMetaClient(zkMetaClientConfig);
      _metaClient.connect();
    } else {
      throw new MetaClientException("Unsupported store type: " + metaClientConfig.getStoreType());
    }
  }

  /**
   * Construct a LeaderElectionClient using a user passed in MetaClient object
   * When MetaClient is auto closed because of being disconnected and auto retry connection timed out, user
   * will need to create a new MetaClient and a new LeaderElectionClient instance.
   *
   * @param metaClient metaClient object to be used.
   */
  public LeaderElectionClient(MetaClientInterface<LeaderInfo> metaClient, String participant) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * Returns true if current participant is the current leadership.
   */
  public boolean isLeader(String leaderPath) {
    return getLeader(leaderPath).equalsIgnoreCase(_participant);
  }

  /**
   * Participants join a leader election group by calling the following API.
   * The Leader Election client maintains and elect an active leader from the participant pool.
   *
   * @param leaderPath The path for leader election.
   * @throws RuntimeException if the operation is not succeeded.
   */
  public void joinLeaderElectionParticipantPool(String leaderPath) {
    // TODO: create participant entry
    subscribeAndTryCreateLeaderEntry(leaderPath);
  }

  /**
   * Participants join a leader election group by calling the following API.
   * The Leader Election client maintains and elect an active leader from the participant pool.
   *
   * @param leaderPath The path for leader election.
   * @param userInfo Any additional information to associate with this participant.
   * @throws RuntimeException if the operation is not succeeded.
   */
  public void joinLeaderElectionParticipantPool(String leaderPath, LeaderInfo userInfo) {
    // TODO: create participant entry with info
    subscribeAndTryCreateLeaderEntry(leaderPath);
  }

  private void subscribeAndTryCreateLeaderEntry(String leaderPath) {
    _metaClient.subscribeDataChange(leaderPath + LEADER_ENTRY_KEY, _reElectListener, false);
    LeaderInfo leaderInfo = new LeaderInfo(LEADER_ENTRY_KEY);
    leaderInfo.setLeaderName(_participant);

    try {
      // try to create leader entry, assuming leader election group node is already there
      _metaClient.create(leaderPath + LEADER_ENTRY_KEY, leaderInfo, MetaClientInterface.EntryMode.EPHEMERAL);
    } catch (MetaClientNodeExistsException ex) {
      LOG.info("Already a leader for group {}", leaderPath);
    } catch (MetaClientNoNodeException ex) {
      try {
        // try to create leader path root entry
        _metaClient.create(leaderPath, null);
      } catch (MetaClientNodeExistsException ignored) {
        // root entry created by other client, ignore
      } catch (MetaClientNoNodeException e) {
        // Parent entry of user provided leader election group path missing.
        // (e.g. `/a/b` not created in user specified leader election group path /a/b/c/LeaderGroup)
        throw new MetaClientException("Parent entry in leaderGroup path" + leaderPath + " does not exist.");
      }
      try {
        // try to create leader node again.
        _metaClient.create(leaderPath + LEADER_ENTRY_KEY, leaderInfo, MetaClientInterface.EntryMode.EPHEMERAL);
      } catch (MetaClientNoNodeException e) {
        // Leader group root entry is gone after we checked at outer catch block.
        // Meaning other client removed the group. Throw ConcurrentModificationException.
        throw new ConcurrentModificationException(
            "Other client trying to modify the leader election group at the same time, please retry.", ex);
      }
    }

    _leaderGroups.add(leaderPath + LEADER_ENTRY_KEY);
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
   * @throws RuntimeException if the operation is not succeeded.
   *
   * @throws RuntimeException If the participant did not join participant pool via this client.
   */
  public void exitLeaderElectionParticipantPool(String leaderPath) {
    _metaClient.unsubscribeDataChange(leaderPath + LEADER_ENTRY_KEY, _reElectListener);
    // TODO: remove from pool folder
    relinquishLeaderHelper(leaderPath, true);
  }

  /**
   * Releases leadership for participant. Still stays in the participant pool.
   *
   * @param leaderPath The path for leader election.
   *
   * @throws RuntimeException if the leadership is not owned by this participant, or if the
   *                          participant did not join participant pool via this client.
   */
  public void relinquishLeader(String leaderPath) {
    relinquishLeaderHelper(leaderPath, false);
  }

  /**
   * relinquishLeaderHelper and LeaderElectionParticipantPool if configured
   * @param leaderPath
   * @param exitLeaderElectionParticipantPool
   */
  private void relinquishLeaderHelper(String leaderPath, Boolean exitLeaderElectionParticipantPool) {
    String key = leaderPath + LEADER_ENTRY_KEY;
    // if current client is in the group
    if (!_leaderGroups.contains(key)) {
      throw new MetaClientException("Participant is not in the leader election group");
    }
    // remove leader path from leaderGroups after check if exiting the pool.
    // to prevent a race condition in In Zk implementation:
    // If there are delays in ZkClient event queue, it is possible the leader election client received leader
    // deleted event after unsubscribeDataChange. We will need to remove it from in memory `leaderGroups` map before
    // deleting ZNode. So that handler in ReElectListener won't recreate the leader node.
    if (exitLeaderElectionParticipantPool) {
      _leaderGroups.remove(leaderPath + LEADER_ENTRY_KEY);
    }
    // check if current participant is the leader
    // read data and stats, check, and multi check + delete
    ImmutablePair<LeaderInfo, MetaClientInterface.Stat> tup = _metaClient.getDataAndStat(key);
    if (tup.left.getLeaderName().equalsIgnoreCase(_participant)) {
      int expectedVersion = tup.right.getVersion();
      List<Op> ops = Arrays.asList(Op.check(key, expectedVersion), Op.delete(key, expectedVersion));
      //Execute transactional support on operations
      List<OpResult> opResults = _metaClient.transactionOP(ops);
      if (opResults.get(0).getType() == ERRORRESULT) {
        if (isLeader(leaderPath)) {
          // Participant re-elected as leader.
          throw new ConcurrentModificationException("Concurrent operation, please retry");
        } else {
          LOG.info("Someone else is already leader");
        }
      }
    }
  }

  /**
   * Get current leader.
   *
   * @param leaderPath The path for leader election.
   * @return Returns the current leader. Return null if no Leader at a given point.
   * @throws RuntimeException when leader path does not exist. // TODO: define exp type
   */
  public String getLeader(String leaderPath) {
    LeaderInfo leaderInfo = _metaClient.get(leaderPath + LEADER_ENTRY_KEY);
    return leaderInfo == null ? null : leaderInfo.getLeaderName();
  }

  public LeaderInfo getParticipantInfo(String leaderPath) {
    // TODO: add getParticipantInfo impl
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
   * A listener will still be installed if the path does not exist yet.
   *
   * @param leaderPath The path for leader election that listener is interested for change.
   * @param listener An implementation of LeaderElectionListenerInterface
   * @return A boolean value indicating if registration is success.
   */
  public boolean subscribeLeadershipChanges(String leaderPath, LeaderElectionListenerInterface listener) {
    //TODO: add converter class for LeaderElectionListenerInterface
    return false;
  }

  /**
   * @param leaderPath The path for leader election that listener is no longer interested for change.
   * @param listener An implementation of LeaderElectionListenerInterface
   */
  public void unsubscribeLeadershipChanges(String leaderPath, LeaderElectionListenerInterface listener) {
  }

  @Override
  public void close() throws Exception {

    // exit all previous joined leader election groups
    for (String leaderGroup : _leaderGroups) {
      String leaderGroupPathName =
          leaderGroup.substring(0, leaderGroup.length() - LEADER_ENTRY_KEY.length() /*remove '/LEADER' */);
      exitLeaderElectionParticipantPool(leaderGroupPathName);
    }

    // TODO: if last participant, remove folder
    _metaClient.disconnect();
  }

  class ReElectListener implements DataChangeListener {

    @Override
    public void handleDataChange(String key, Object data, ChangeType changeType) throws Exception {
      if (changeType == ChangeType.ENTRY_CREATED) {
        LOG.info("new leader {} for leader election group {}.", ((LeaderInfo) data).getLeaderName(), key);
      } else if (changeType == ChangeType.ENTRY_DELETED) {
        if (_leaderGroups.contains(key)) {
          LeaderInfo lf = new LeaderInfo("LEADER");
          lf.setLeaderName(_participant);
          try {
            _metaClient.create(key, lf, MetaClientInterface.EntryMode.EPHEMERAL);
          } catch (MetaClientNodeExistsException ex) {
            LOG.info("Already a leader {} for leader election group {}.", ((LeaderInfo) data).getLeaderName(), key);
          }
        }
      }
    }
  }
}

