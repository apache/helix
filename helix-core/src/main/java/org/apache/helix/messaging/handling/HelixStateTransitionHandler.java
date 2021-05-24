package org.apache.helix.messaging.handling;

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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRollbackException;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.MapKey;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.task.TaskStateModel;
import org.apache.helix.util.StatusUpdateUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.ZNRecordBucketizer;
import org.apache.helix.zookeeper.datamodel.ZNRecordDelta;
import org.apache.helix.zookeeper.datamodel.ZNRecordDelta.MergeOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelixStateTransitionHandler extends MessageHandler {
  public static class HelixStateMismatchException extends Exception {
    public HelixStateMismatchException(String info) {
      super(info);
    }
  }

  /**
   * If current state == toState in message, this is considered as Duplicated state transition
   */
  public static class HelixDuplicatedStateTransitionException extends Exception {
    public HelixDuplicatedStateTransitionException(String info) {
      super(info);
    }
  }

  public static class StaleMessageValidateResult {
    public boolean isValid;
    public Exception exception;

    StaleMessageValidateResult(Exception exp) {
      exception = exp;
      isValid = exception == null;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(HelixStateTransitionHandler.class);
  private final StateModel _stateModel;
  StatusUpdateUtil _statusUpdateUtil;
  private final StateModelParser _transitionMethodFinder;
  private final CurrentState _currentStateDelta;
  private final HelixManager _manager;
  private final StateModelFactory<? extends StateModel> _stateModelFactory;
  private final boolean _isTaskMessage;
  private final boolean _isTaskCurrentStatePathDisabled;
  volatile boolean _isTimeout = false;

  public HelixStateTransitionHandler(StateModelFactory<? extends StateModel> stateModelFactory,
      StateModel stateModel, Message message, NotificationContext context,
      CurrentState currentStateDelta) {
    super(message, context);
    _stateModel = stateModel;
    _statusUpdateUtil = new StatusUpdateUtil();
    _transitionMethodFinder = new StateModelParser();
    _currentStateDelta = currentStateDelta;
    _manager = _notificationContext.getManager();
    _stateModelFactory = stateModelFactory;
    _isTaskMessage = stateModel instanceof TaskStateModel;
    _isTaskCurrentStatePathDisabled =
        Boolean.getBoolean(SystemPropertyKeys.TASK_CURRENT_STATE_PATH_DISABLED);
  }

  void preHandleMessage() throws Exception {
    if (!_message.isValid()) {
      String errorMessage = "Invalid Message, ensure that message: " + _message
          + " has all the required fields: " + Arrays.toString(Message.Attributes.values());

      _statusUpdateUtil.logError(_message, HelixStateTransitionHandler.class, errorMessage,
          _manager);
      logger.error(errorMessage);
      throw new HelixException(errorMessage);
    }

    logger.info("handling message: " + _message.getMsgId() + " transit "
        + _message.getResourceName() + "." + _message.getPartitionName() + "|"
        + _message.getPartitionNames() + " from:" + _message.getFromState() + " to:"
        + _message.getToState() + ", relayedFrom: " + _message.getRelaySrcHost());

    // Set start time right before invoke client logic
    _currentStateDelta.setStartTime(_message.getPartitionName(), System.currentTimeMillis());

    StaleMessageValidateResult err = staleMessageValidator();
    if (!err.isValid) {
      _statusUpdateUtil
          .logError(_message, HelixStateTransitionHandler.class, err.exception.getMessage(),
              _manager);
      logger.error(err.exception.getMessage());
      throw err.exception;
    }

  }

  void postHandleMessage() {
    HelixTaskResult taskResult =
        (HelixTaskResult) _notificationContext.get(MapKey.HELIX_TASK_RESULT.toString());
    Exception exception = taskResult.getException();
    String partitionKey = _message.getPartitionName();

    // No need to sync on manager, we are cancel executor in expiry session before start executor in
    // new session
    // sessionId might change when we update the state model state.
    // for zk current state it is OK as we have the per-session current state node
    if (!_message.getTgtSessionId().equals(_manager.getSessionId())) {
      logger.warn("Session id has changed. Skip postExecutionMessage. Old session " + _message
          .getExecutionSessionId() + " , new session : " + _manager.getSessionId());
      return;
    }

    // Set the INFO property and mark the end time, previous state of the state transition
    _currentStateDelta.setInfo(partitionKey, taskResult.getInfo());
    _currentStateDelta.setEndTime(partitionKey, taskResult.getCompleteTime());
    _currentStateDelta.setPreviousState(partitionKey, _message.getFromState());

    // add host name this state transition is triggered by.
    _currentStateDelta.setTriggerHost(partitionKey,
        Message.MessageType.RELAYED_MESSAGE.name().equals(_message.getMsgSubType()) ?
            _message.getRelaySrcHost() : _message.getMsgSrc());

    if (taskResult.isSuccess()) {
      // String fromState = message.getFromState();
      String toState = _message.getToState();
      _currentStateDelta.setState(partitionKey, toState);
      if (toState.equalsIgnoreCase(HelixDefinedState.DROPPED.toString())) {
        // for "OnOfflineToDROPPED" message, we need to remove the resource key record
        // from the current state of the instance because the resource key is dropped.
        // In the state model it will be stayed as "OFFLINE", which is OK.
        ZNRecord rec = new ZNRecord(_currentStateDelta.getId());
        rec.getMapFields().put(partitionKey, null);
        ZNRecordDelta delta = new ZNRecordDelta(rec, MergeOperation.SUBTRACT);

        List<ZNRecordDelta> deltaList = new ArrayList<>();
        deltaList.add(delta);
        _currentStateDelta.setDeltaList(deltaList);
        _stateModelFactory.removeStateModel(_message.getResourceName(), partitionKey);
      } else if (_stateModel.getCurrentState().equals(_message.getFromState())) {
        // if the partition is not to be dropped, update _stateModel to the TO_STATE
        // need this check because TaskRunner may change _stateModel before reach here.
        _stateModel.updateState(toState);
      }
    } else if (!taskResult.isCancelled()) {
      if (exception instanceof HelixStateMismatchException) {
        // if fromState mismatch, set current state on zk to stateModel's current state
        logger.warn("Force CurrentState on Zk to be stateModel's CurrentState. partitionKey: "
            + partitionKey + ", currentState: " + _stateModel.getCurrentState() + ", message: "
            + _message);
        _currentStateDelta.setState(partitionKey, _stateModel.getCurrentState());
        updateZKCurrentState();
        return;
      }
      // Handle timeout interrupt.
      if ((exception instanceof InterruptedException) && !_isTimeout) {
        // State transition interrupted but not caused by timeout. Keep the current
        // state in this case
        logger.error(
            "State transition interrupted but not timeout. Not updating state. Partition : "
                + _message.getPartitionName() + " MsgId : " + _message.getMsgId());
        return;
      }
      // We did early return when Timeout interrupt.
      StateTransitionError error = new StateTransitionError(ErrorType.INTERNAL,
          exception instanceof InterruptedException ? ErrorCode.TIMEOUT : ErrorCode.ERROR,
          exception);
      _stateModel.rollbackOnError(_message, _notificationContext, error);
      _currentStateDelta.setState(partitionKey, HelixDefinedState.ERROR.toString());
      _stateModel.updateState(HelixDefinedState.ERROR.toString());
    } // NOP if taskResult.isCancelled()

    updateZKCurrentState();
  }

  // Update the ZK current state of the node
  private void updateZKCurrentState(){
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    String partitionKey = _message.getPartitionName();
    String resource = _message.getResourceName();
    String sessionId = _message.getTgtSessionId();
    String instanceName = _manager.getInstanceName();

    try {
      // We did not update _stateModel for DROPPED state, so it won't match _stateModel.
      if (!_stateModel.getCurrentState().equals(_currentStateDelta.getState(partitionKey))
          && !_currentStateDelta.getState(partitionKey)
          .equalsIgnoreCase(HelixDefinedState.DROPPED.toString())) {
        logger.warn("_stateModel is already updated by TaskRunner. Skip ZK update in StateTransitionHandler");
        return;
      }
      int bucketSize = _message.getBucketSize();
      ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(bucketSize);
      PropertyKey key = _isTaskMessage && !_isTaskCurrentStatePathDisabled ? accessor.keyBuilder()
          .taskCurrentState(instanceName, sessionId, resource,
              bucketizer.getBucketName(partitionKey)) : accessor.keyBuilder()
          .currentState(instanceName, sessionId, resource, bucketizer.getBucketName(partitionKey));
      if (_message.getAttribute(Attributes.PARENT_MSG_ID) == null) {
        // normal message
        if (!accessor.updateProperty(key, _currentStateDelta)) {
          throw new HelixException(
              "Fails to persist current state back to ZK for resource " + resource + " partition: "
                  + _message.getPartitionName());
        }
      } else {
        // sub-message of a batch message
        ConcurrentHashMap<String, CurrentStateUpdate> csUpdateMap =
            (ConcurrentHashMap<String, CurrentStateUpdate>) _notificationContext
                .get(MapKey.CURRENT_STATE_UPDATE.toString());
        csUpdateMap.put(partitionKey, new CurrentStateUpdate(key, _currentStateDelta));
      }
    } catch (Exception e) {
      logger.error("Error when updating current-state ", e);
      StateTransitionError error =
          new StateTransitionError(ErrorType.FRAMEWORK, ErrorCode.ERROR, e);
      _stateModel.rollbackOnError(_message, _notificationContext, error);
      _statusUpdateUtil.logError(_message, HelixStateTransitionHandler.class, e,
          "Error when update current-state ", _manager);
    }


  }

  @Override
  public HelixTaskResult handleMessage() {
    NotificationContext context = _notificationContext;
    Message message = _message;

    HelixTaskResult taskResult = new HelixTaskResult();
    HelixManager manager = context.getManager();

    _statusUpdateUtil
        .logInfo(message, HelixStateTransitionHandler.class, "Message handling task begin execute",
            manager);
    message.setExecuteStartTimeStamp(System.currentTimeMillis());

    try {
      preHandleMessage();
      invoke(manager, context, taskResult, message);
    } catch (HelixDuplicatedStateTransitionException e) {
      // Duplicated state transition problem is fine
      taskResult.setSuccess(true);
      taskResult.setMessage(e.toString());
      taskResult.setInfo(e.getMessage());
    } catch (HelixStateMismatchException e) {
      // Simply log error and return from here if State mismatch.
      // The current state of the state model is intact.
      taskResult.setSuccess(false);
      taskResult.setMessage(e.toString());
      taskResult.setException(e);
    } catch (Exception e) {
      String errorMessage =
          "Exception while executing a state transition task " + message.getPartitionName();
      logger.error(errorMessage, e);
      if (e.getCause() != null && e.getCause() instanceof InterruptedException) {
        e = (InterruptedException) e.getCause();
      }

      if (e instanceof HelixRollbackException || (e.getCause() != null && e
          .getCause() instanceof HelixRollbackException)) {
        // TODO : Support cancel to any state
        logger.info(
            "Rollback happened of state transition on resource \"" + _message.getResourceName()
                + "\" partition \"" + _message.getPartitionName() + "\" from \"" + _message
                .getFromState() + "\" to \"" + _message.getToState() + "\"");
        taskResult.setCancelled(true);
      } else {
        _statusUpdateUtil
            .logError(message, HelixStateTransitionHandler.class, e, errorMessage, manager);
        taskResult.setSuccess(false);
        taskResult.setMessage(e.toString());
        taskResult.setException(e);
        taskResult.setInterrupted(e instanceof InterruptedException);
      }
    }
    taskResult.setCompleteTime(System.currentTimeMillis());
    // add task result to context for postHandling
    context.add(MapKey.HELIX_TASK_RESULT.toString(), taskResult);
    synchronized (_stateModel) {
      postHandleMessage();
    }
    return taskResult;
  }

  private void invoke(HelixManager manager, NotificationContext context, HelixTaskResult taskResult,
      Message message) throws IllegalAccessException, InvocationTargetException,
      InterruptedException, HelixRollbackException {
    _statusUpdateUtil.logInfo(message, HelixStateTransitionHandler.class,
        "Message handling invoking", manager);

    // by default, we invoke state transition function in state model
    Method methodToInvoke = null;
    String fromState = message.getFromState();
    String toState = message.getToState();
    methodToInvoke = _transitionMethodFinder.getMethodForTransition(_stateModel.getClass(),
        fromState, toState, new Class[] {
            Message.class, NotificationContext.class
        });
    if (methodToInvoke != null) {
      logger.info(String.format(
          "Instance %s, partition %s received state transition from %s to %s on session %s, message id: %s",
          message.getTgtName(), message.getPartitionName(), message.getFromState(),
          message.getToState(), message.getTgtSessionId(), message.getMsgId()));
      if (_cancelled) {
        throw new HelixRollbackException(String.format(
            "Instance %s, partition %s state transition from %s to %s on session %s has been cancelled, message id: %s",
            message.getTgtName(), message.getPartitionName(), message.getFromState(),
            message.getToState(), message.getTgtSessionId(), message.getMsgId()));
      }

      Object result = methodToInvoke.invoke(_stateModel, new Object[] {
          message, context
      });
      taskResult.setSuccess(true);
      String resultStr;
      if (result == null || result instanceof Void) {
        resultStr = "";
      } else {
        resultStr = result.toString();
      }
      taskResult.setInfo(resultStr);
    } else {
      String errorMessage = "Unable to find method for transition from " + fromState + " to "
          + toState + " in " + _stateModel.getClass();
      logger.error(errorMessage);
      taskResult.setSuccess(false);
      taskResult.setInfo(errorMessage);
      _statusUpdateUtil.logError(message, HelixStateTransitionHandler.class, errorMessage, manager);
    }
  }

  @Override
  public void onError(Exception e, ErrorCode code, ErrorType type) {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    String instanceName = _manager.getInstanceName();
    String resourceName = _message.getResourceName();
    String partition = _message.getPartitionName();

    // All internal error has been processed already, so we can skip them
    if (type == ErrorType.INTERNAL) {
      logger.error("Skip internal error. errCode: " + code + ", errMsg: " + e.getMessage());
      return;
    }

    try {
      // set current state to ERROR for the partition
      // if the transition is not canceled, it should go into error state
      if (code == ErrorCode.ERROR) {
        CurrentState currentStateDelta = new CurrentState(resourceName);
        currentStateDelta.setState(partition, HelixDefinedState.ERROR.toString());
        _stateModel.updateState(HelixDefinedState.ERROR.toString());

        PropertyKey currentStateKey =
            _isTaskMessage && !_isTaskCurrentStatePathDisabled ? keyBuilder
                .taskCurrentState(instanceName, _message.getTgtSessionId(), resourceName)
                : keyBuilder.currentState(instanceName, _message.getTgtSessionId(), resourceName);
        if (!accessor.updateProperty(currentStateKey, currentStateDelta)) {
          logger.error("Fails to persist ERROR current state to ZK for resource " + resourceName
              + " partition: " + partition);
        }
      }
    } finally {
      StateTransitionError error = new StateTransitionError(type, code, e);
      _stateModel.rollbackOnError(_message, _notificationContext, error);
    }
  }

  // Verify the fromState and current state of the stateModel.
  public StaleMessageValidateResult staleMessageValidator() {
    String fromState = _message.getFromState();
    String toState = _message.getToState();
    String partitionName = _message.getPartitionName();

    // state in _currentStateDelta uses current state from state model. It has the
    // most up-to-date. current state. In case currentState in stateModel is null,
    // partition is in initial state and we using it as current state.
    // Defined in HelixStateMachineEngine.
    String state = _currentStateDelta.getState(partitionName);

    Exception err = null;
    if (toState.equalsIgnoreCase(state)) {
      // To state equals current state, we can just ignore the message
      err = new HelixDuplicatedStateTransitionException(String
          .format("Partition %s current state is same as toState (%s->%s) from message.",
              partitionName, fromState, toState));
    } else if (fromState != null && !fromState.equals("*") && !fromState.equalsIgnoreCase(state)) {
      // If current state is neither toState nor fromState in message, there is a problem
      err = new HelixStateMismatchException(String.format(
          "Current state of stateModel does not match the fromState in Message, CurrentState: %s, Message: %s->%s, Partition: %s, from: %s, to: %s",
          state, fromState, toState, partitionName, _message.getMsgSrc(), _message.getTgtName()));
    }
    return new StaleMessageValidateResult(err);
  }

  @Override
  public void onTimeout() {
    _isTimeout = true;
  }
}
