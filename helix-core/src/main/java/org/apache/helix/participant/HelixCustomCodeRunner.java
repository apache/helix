package org.apache.helix.participant;

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
import java.util.Arrays;
import java.util.List;

import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.api.id.StateModelFactoryId;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.log4j.Logger;

/**
 * This provides the ability for users to run a custom code in exactly one
 * process using a LeaderStandBy state model. <br/>
 * A typical use case is when one uses CUSTOMIZED ideal state mode where the
 * assignment of partition to nodes needs to change dynamically as the nodes go
 * online/offline.<br/>
 * <code>
 * HelixCustomCodeRunner runner = new HelixCustomCodeRunner(manager,ZK_ADDR);
 * runner
 *  .invoke(_callback)
 *  .on(ChangeType.LIVE_INSTANCE, ChangeType.IdealState)
 *  .usingLeaderStandbyModel("someUniqueId")
 *  .start()
 * </code>
 */
public class HelixCustomCodeRunner {
  private static Logger LOG = Logger.getLogger(HelixCustomCodeRunner.class);
  private static String PARTICIPANT_LEADER = "PARTICIPANT_LEADER";

  private CustomCodeCallbackHandler _callback;
  private List<ChangeType> _notificationTypes;
  private String _resourceName;
  private final HelixManager _manager;
  private final String _zkAddr;
  private GenericLeaderStandbyStateModelFactory _stateModelFty;

  /**
   * Constructs a HelixCustomCodeRunner that will run exactly in one place
   * @param manager
   * @param zkAddr
   */
  public HelixCustomCodeRunner(HelixManager manager, String zkAddr) {
    _manager = manager;
    _zkAddr = zkAddr;
  }

  /**
   * callback to invoke when there is a change in cluster state specified by on(
   * notificationTypes) This callback must be idempotent which means they should
   * not depend on what changed instead simply read the cluster data and act on
   * it.
   * @param callback
   * @return
   */
  public HelixCustomCodeRunner invoke(CustomCodeCallbackHandler callback) {
    _callback = callback;
    return this;
  }

  /**
   * ChangeTypes interested in, ParticipantLeaderCallback.callback method will
   * be invoked on the
   * @param notificationTypes
   * @return
   */
  public HelixCustomCodeRunner on(ChangeType... notificationTypes) {
    _notificationTypes = Arrays.asList(notificationTypes);
    return this;
  }

  public HelixCustomCodeRunner usingLeaderStandbyModel(String id) {
    _resourceName = PARTICIPANT_LEADER + "_" + id;
    return this;
  }

  /**
   * Get resource name for the custom-code runner
   * Used for retrieving the external view for the custom-code runner resource
   * @return resource name for the custom-code runner
   */
  public String getResourceName() {
    return _resourceName;
  }

  /**
   * This method will be invoked when there is a change in any subscribed
   * notificationTypes
   * @throws Exception
   */
  public void start() throws Exception {
    if (_callback == null || _notificationTypes == null || _notificationTypes.size() == 0
        || _resourceName == null) {
      throw new IllegalArgumentException("Require callback | notificationTypes | resourceName");
    }

    LOG.info("Register participantLeader on " + _notificationTypes + " using " + _resourceName);

    _stateModelFty = new GenericLeaderStandbyStateModelFactory(_callback, _notificationTypes);

    StateMachineEngine stateMach = _manager.getStateMachineEngine();
    stateMach.registerStateModelFactory(StateModelDefId.LeaderStandby, _resourceName,
        _stateModelFty);
    ZkClient zkClient = null;
    try {
      // manually add ideal state for participant leader using LeaderStandby
      // model

      zkClient =
          new ZkClient(_zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      HelixDataAccessor accessor =
          new ZKHelixDataAccessor(_manager.getClusterName(), new ZkBaseDataAccessor<ZNRecord>(
              zkClient));
      Builder keyBuilder = accessor.keyBuilder();

      IdealState idealState = new IdealState(_resourceName);
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setReplicas(StateModelToken.ANY_LIVEINSTANCE.toString());
      idealState.setNumPartitions(1);
      idealState.setStateModelDefId(StateModelDefId.LeaderStandby);
      idealState.setStateModelFactoryId(StateModelFactoryId.from(_resourceName));
      List<String> prefList =
          new ArrayList<String>(Arrays.asList(StateModelToken.ANY_LIVEINSTANCE.toString()));
      idealState.getRecord().setListField(_resourceName + "_0", prefList);

      List<String> idealStates = accessor.getChildNames(keyBuilder.idealStates());
      while (idealStates == null || !idealStates.contains(_resourceName)) {
        accessor.setProperty(keyBuilder.idealStates(_resourceName), idealState);
        idealStates = accessor.getChildNames(keyBuilder.idealStates());
      }

      LOG.info("Set idealState for participantLeader:" + _resourceName + ", idealState:"
          + idealState);
    } finally {
      if (zkClient != null && zkClient.getConnection() != null)

      {
        zkClient.close();
      }
    }

  }

  /**
   * Stop customer code runner
   */
  public void stop() {
    LOG.info("Removing stateModelFactory for " + _resourceName);
    _manager.getStateMachineEngine().removeStateModelFactory(StateModelDefId.LeaderStandby,
        _resourceName);
  }
}
