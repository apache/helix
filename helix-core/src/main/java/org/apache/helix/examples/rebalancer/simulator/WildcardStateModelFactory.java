package org.apache.helix.examples.rebalancer.simulator;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

/**
 * A wild card state model that can react on any state transition.
 * This class is for simulation only.
 */
public class WildcardStateModelFactory extends StateModelFactory<StateModel> {
  final int _delay;
  final String _instanceName;
  final boolean _verbose;

  final Map<String, Integer> _transitionCount = new HashMap<>();

  public synchronized Map<String, Integer> getAndResetTransitionRecords() {
    Map<String, Integer> ret = new HashMap<>(_transitionCount);
    _transitionCount.clear();
    return ret;
  }

  public synchronized Integer getTransitionCount() {
    return _transitionCount.values().stream().mapToInt(Integer::intValue).sum();
  }

  private synchronized void updateTransitionRecord(String fromState, String toState) {
    String desc = fromState + " -> " + toState;
    _transitionCount.compute(desc, (k, v) -> (v == null) ? 1 : (v + 1));
  }

  public WildcardStateModelFactory(String instanceName, int delay, boolean verbose) {
    _instanceName = instanceName;
    _delay = delay;
    _verbose = verbose;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    WildCardStateModel stateModel = new WildCardStateModel(this, _verbose);
    stateModel.setInstanceName(_instanceName);
    stateModel.setDelay(_delay);
    stateModel.setPartitionName(partitionName);
    return stateModel;
  }

  @StateModelInfo(states = "{}", initialState = "OFFLINE")
  public static class WildCardStateModel extends StateModel {
    int _transDelay = 0;
    String partitionName;
    String _instanceName = "";
    WildcardStateModelFactory _factory;
    boolean _verbose;

    WildCardStateModel(WildcardStateModelFactory factory, boolean verbose) {
      _factory = factory;
      _verbose = verbose;
    }

    String getPartitionName() {
      return partitionName;
    }

    void setPartitionName(String partitionName) {
      this.partitionName = partitionName;
    }

    void setDelay(int delay) {
      _transDelay = delay > 0 ? delay : 0;
    }

    void setInstanceName(String instanceName) {
      _instanceName = instanceName;
    }

    @Transition(to = "*", from = "*")
    public void defaultTransitionHandler(Message message, NotificationContext context) {
      if (_verbose) {
        System.out.println(
            _instanceName + " transitioning from " + message.getFromState() + " to " + message
                .getToState() + " for " + message.getPartitionName());
      }
      _factory.updateTransitionRecord(message.getFromState(), message.getToState());
      sleep();
    }

    private void sleep() {
      try {
        Thread.sleep(_transDelay);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
