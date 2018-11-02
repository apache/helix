package org.apache.helix.view.statemodel;

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

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.AbstractHelixLeaderStandbyStateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.view.aggregator.HelixViewAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(initialState = "OFFLINE", states = {
    "LEADER", "STANDBY"
})
public class DistViewAggregatorStateModel extends AbstractHelixLeaderStandbyStateModel {
  private final static Logger logger = LoggerFactory.getLogger(DistViewAggregatorStateModel.class);
  private final static Object stateTransitionLock = new Object();
  private static HelixViewAggregator _aggregator;

  public DistViewAggregatorStateModel(String zkAddr) {
    super(zkAddr);
  }

  @Override
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    logStateTransition("OFFLINE", "STANDBY", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception {
    String viewClusterName = message.getPartitionName();

    synchronized (stateTransitionLock) {
      if (_aggregator != null) {
        logger.warn("Aggregator already exists for view cluster {}: {}: cleaning it up.",
            viewClusterName, _aggregator.getAggregatorInstanceName());
        reset();
      }
      logger.info("Creating new HelixViewAggregator for view cluster {}", viewClusterName);
      try {
        _aggregator = new HelixViewAggregator(viewClusterName, _zkAddr);
        _aggregator.start();
      } catch (Exception e) {
        logger.error("Aggregator failed to become leader from stand by for view cluster {}",
            viewClusterName, e);
        reset();
        throw e;
      }
      logStateTransition("STANDBY", "LEADER", message.getPartitionName(), message.getTgtName());
    }

  }

  @Override
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    reset();
    logStateTransition("LEADER", "STANDBY", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    logStateTransition("OFFLINE", "DROPPED", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    logStateTransition("OFFLINE", "DROPPED", message.getPartitionName(), message.getTgtName());
  }


  @Override
  public void reset() {
    synchronized (stateTransitionLock) {
      if (_aggregator != null) {
        logger.error("Resetting view aggregator {}", _aggregator.getAggregatorInstanceName());
        _aggregator.shutdown();
        _aggregator = null;
      }
    }
  }

  @Override
  public String getStateModeInstanceDescription(String partitionName, String instanceName) {
    return String.format("View Aggregator (%s) for view cluster %s", instanceName, partitionName);
  }
}
