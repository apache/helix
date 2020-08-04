package org.apache.helix.manager.zk;

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

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.zookeeper.zkclient.DataUpdater;


/**
 * updater for carrying over last current states
 * @see HELIX-30: ZkHelixManager.carryOverPreviousCurrentState() should use a special merge logic
 *      because carryOver() is performed after addLiveInstance(). it's possible that carryOver()
 *      overwrites current-state updates performed by current session. so carryOver() should be
 *      performed only when current-state is empty for the partition
 */
class CurStateCarryOverUpdater implements DataUpdater<ZNRecord> {
  final String _curSessionId;
  final String _initState;
  final CurrentState _lastCurState;

  public CurStateCarryOverUpdater(String curSessionId, String initState, CurrentState lastCurState) {
    if (curSessionId == null || initState == null || lastCurState == null) {
      throw new IllegalArgumentException(
          "missing curSessionId|initState|lastCurState for carry-over");
    }
    _curSessionId = curSessionId;
    _initState = initState;
    _lastCurState = lastCurState;
  }

  @Override
  public ZNRecord update(ZNRecord currentData) {
    CurrentState curState;
    if (currentData == null) {
      curState = new CurrentState(_lastCurState.getId());
      // copy all simple fields settings and overwrite session-id to current session
      curState.getRecord().setSimpleFields(_lastCurState.getRecord().getSimpleFields());
      curState.setSessionId(_curSessionId);
    } else {
      curState = new CurrentState(currentData);
    }

    for (String partitionName : _lastCurState.getPartitionStateMap().keySet()) {
      // For tasks, we preserve previous session's CurrentStates and set RequestState to DROPPED so
      // that they will be dropped by the Controller
      if (_lastCurState.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)) {
        curState.setState(partitionName, _lastCurState.getState(partitionName));
        curState.setRequestedState(partitionName, TaskPartitionState.DROPPED.name());
      } else {
        // carry-over only when current-state does not exist for regular Helix resource partitions
        if (curState.getState(partitionName) == null) {
          curState.setState(partitionName, _initState);
        }
      }
    }
    return curState.getRecord();
  }

}
