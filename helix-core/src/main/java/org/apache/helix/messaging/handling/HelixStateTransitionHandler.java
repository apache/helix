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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.MapKey;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZNRecordBucketizer;
import org.apache.helix.ZNRecordDelta;
import org.apache.helix.ZNRecordDelta.MergeOperation;
import org.apache.helix.api.State;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.util.StatusUpdateUtil;
import org.apache.log4j.Logger;

public class HelixStateTransitionHandler extends MessageHandler {
  public static class HelixStateMismatchException extends Exception {
    private static final long serialVersionUID = -7669959598697794766L;

    public HelixStateMismatchException(String info) {
      super(info);
    }
  }

  private static final Logger logger = Logger.getLogger(HelixStateTransitionHandler.class);
  private final TransitionHandler _stateModel;
  StatusUpdateUtil _statusUpdateUtil;
  private final StateModelParser _transitionMethodFinder;
  private final CurrentState _currentStateDelta;
  private final HelixManager _manager;
  private final StateTransitionHandlerFactory<? extends TransitionHandler> _stateModelFactory;
  volatile boolean _isTimeout = false;

  public HelixStateTransitionHandler(StateTransitionHandlerFactory<? extends TransitionHandler> stateModelFactory,
      TransitionHandler stateModel, Message message, NotificationContext context,
      CurrentState currentStateDelta) {
    super(message, context);
    _stateModel = stateModel;
    _statusUpdateUtil = new StatusUpdateUtil();
    _transitionMethodFinder = new StateModelParser();
    _currentStateDelta = currentStateDelta;
    _manager = _notificationContext.getManager();
    _stateModelFactory = stateModelFactory;
  }

  void preHandleMessage() throws Exception {
    if (!_message.isValid()) {
      String errorMessage =
          "Invalid Message, ensure that message: " + _message + " has all the required fields: "
              + Arrays.toString(Message.Attributes.values());

      _statusUpdateUtil.logError(_message, HelixStateTransitionHandler.class, errorMessage,
          _manager.getHelixDataAccessor());
      logger.error(errorMessage);
      throw new HelixException(errorMessage);
    }

    HelixDataAccessor accessor = _manager.getHelixDataAccessor();

    PartitionId partitionId = _message.getPartitionId();
    State fromState = _message.getTypedFromState();

    // Verify the fromState and current state of the stateModel
    String state = _currentStateDelta.getState(partitionId.stringify());

    if (fromState != null && !fromState.equals("*")
        && !fromState.toString().equalsIgnoreCase(state)) {
      String errorMessage =
          "Current state of stateModel does not match the fromState in Message"
              + ", Current State:" + state + ", message expected:" + fromState + ", partition: "
              + partitionId + ", from: " + _message.getMsgSrc() + ", to: " + _message.getTgtName();

      _statusUpdateUtil.logError(_message, HelixStateTransitionHandler.class, errorMessage,
          accessor);
      logger.error(errorMessage);
      throw new HelixStateMismatchException(errorMessage);
    }

    /**
     * Reset the REQUESTED_STATE property if it exists.
     * ideally we should merge all current-state update into one zk-write
     */
    if (_stateModel.getRequestedState() != null) {
      try {
        String instance = _manager.getInstanceName();
        String sessionId = _message.getTgtSessionId();
        String resource = _message.getResourceName();
        String partitionName = _message.getPartitionName();
        ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(_message.getBucketSize());
        PropertyKey key =
            accessor.keyBuilder().currentState(instance, sessionId, resource,
                bucketizer.getBucketName(partitionName));
        ZNRecord rec = new ZNRecord(resource);
        Map<String, String> map = new TreeMap<String, String>();
        map.put(CurrentState.CurrentStateProperty.REQUESTED_STATE.name(), null);
        rec.getMapFields().put(partitionName, map);
        ZNRecordDelta delta = new ZNRecordDelta(rec, ZNRecordDelta.MergeOperation.SUBTRACT);
        List<ZNRecordDelta> deltaList = new ArrayList<ZNRecordDelta>();
        deltaList.add(delta);
        CurrentState currStateUpdate = new CurrentState(resource);
        currStateUpdate.setDeltaList(deltaList);
        // Update the ZK current state of the node
        accessor.updateProperty(key, currStateUpdate);
        _stateModel.setRequestedState(null);
      } catch (Exception e) {
        logger.error(
            "Error when removing " + CurrentState.CurrentStateProperty.REQUESTED_STATE.name()
                + " from current state.", e);
        StateTransitionError error =
            new StateTransitionError(ErrorType.FRAMEWORK, ErrorCode.ERROR, e);
        _stateModel.rollbackOnError(_message, _notificationContext, error);
        _statusUpdateUtil.logError(_message, HelixStateTransitionHandler.class, e,
            "Error when removing " + CurrentState.CurrentStateProperty.REQUESTED_STATE.name()
                + " from current state.", accessor);
      }
    }
  }

  void postHandleMessage() {
    HelixTaskResult taskResult =
        (HelixTaskResult) _notificationContext.get(MapKey.HELIX_TASK_RESULT.toString());
    Exception exception = taskResult.getException();

    PartitionId partitionId = _message.getPartitionId();
    ResourceId resource = _message.getResourceId();
    SessionId sessionId = _message.getTypedTgtSessionId();
    String instanceName = _manager.getInstanceName();

    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    int bucketSize = _message.getBucketSize();
    ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(bucketSize);

    // No need to sync on manager, we are cancel executor in expiry session before start executor in
    // new session
    // sessionId might change when we update the state model state.
    // for zk current state it is OK as we have the per-session current state node
    if (!_message.getTypedTgtSessionId().stringify().equals(_manager.getSessionId())) {
      logger.warn("Session id has changed. Skip postExecutionMessage. Old session "
          + _message.getTypedExecutionSessionId() + " , new session : " + _manager.getSessionId());
      return;
    }

    // Set the INFO property.
    _currentStateDelta.setInfo(partitionId, taskResult.getInfo());

    if (taskResult.isSuccess()) {
      // String fromState = message.getFromState();
      State toState = _message.getTypedToState();
      _currentStateDelta.setState(partitionId, toState);

      if (toState.toString().equalsIgnoreCase(HelixDefinedState.DROPPED.toString())) {
        // for "OnOfflineToDROPPED" message, we need to remove the resource key record
        // from the current state of the instance because the resource key is dropped.
        // In the state model it will be stayed as "OFFLINE", which is OK.

        ZNRecord rec = new ZNRecord(_currentStateDelta.getId());
        // remove mapField keyed by partitionId
        rec.setMapField(partitionId.stringify(), null);
        ZNRecordDelta delta = new ZNRecordDelta(rec, MergeOperation.SUBTRACT);

        List<ZNRecordDelta> deltaList = new ArrayList<ZNRecordDelta>();
        deltaList.add(delta);
        _currentStateDelta.setDeltaList(deltaList);
        _stateModelFactory.removeTransitionHandler(resource, partitionId);
      } else {
        // if the partition is not to be dropped, update _stateModel to the TO_STATE
        _stateModel.updateState(toState.toString());
      }
    } else {
      if (exception instanceof HelixStateMismatchException) {
        // if fromState mismatch, set current state on zk to stateModel's current state
        logger.warn("Force CurrentState on Zk to be stateModel's CurrentState. partitionKey: "
            + partitionId + ", currentState: " + _stateModel.getCurrentState() + ", message: "
            + _message);
        _currentStateDelta.setState(partitionId, State.from(_stateModel.getCurrentState()));
      } else {
        StateTransitionError error =
            new StateTransitionError(ErrorType.INTERNAL, ErrorCode.ERROR, exception);
        if (exception instanceof InterruptedException) {
          if (_isTimeout) {
            error = new StateTransitionError(ErrorType.INTERNAL, ErrorCode.TIMEOUT, exception);
          } else {
            // State transition interrupted but not caused by timeout. Keep the current
            // state in this case
            logger
                .error("State transition interrupted but not timeout. Not updating state. Partition : "
                    + _message.getPartitionId() + " MsgId : " + _message.getMessageId());
            return;
          }
        }
        _stateModel.rollbackOnError(_message, _notificationContext, error);
        _currentStateDelta.setState(partitionId, State.from(HelixDefinedState.ERROR.toString()));
        _stateModel.updateState(HelixDefinedState.ERROR.toString());

        // if we have errors transit from ERROR state, disable the partition
        if (_message.getTypedFromState().toString()
            .equalsIgnoreCase(HelixDefinedState.ERROR.toString())) {
          disablePartition();
        }
      }
    }

    try {
      // Update the ZK current state of the node
      PropertyKey key =
          keyBuilder.currentState(instanceName, sessionId.stringify(), resource.stringify(),
              bucketizer.getBucketName(partitionId.stringify()));
      if (_message.getAttribute(Attributes.PARENT_MSG_ID) == null) {
        // normal message
        accessor.updateProperty(key, _currentStateDelta);
      } else {
        // sub-message of a batch message
        ConcurrentHashMap<String, CurrentStateUpdate> csUpdateMap =
            (ConcurrentHashMap<String, CurrentStateUpdate>) _notificationContext
                .get(MapKey.CURRENT_STATE_UPDATE.toString());
        csUpdateMap.put(partitionId.stringify(), new CurrentStateUpdate(key, _currentStateDelta));
      }
    } catch (Exception e) {
      logger.error("Error when updating current-state ", e);
      StateTransitionError error =
          new StateTransitionError(ErrorType.FRAMEWORK, ErrorCode.ERROR, e);
      _stateModel.rollbackOnError(_message, _notificationContext, error);
      _statusUpdateUtil.logError(_message, HelixStateTransitionHandler.class, e,
          "Error when update current-state ", accessor);
    }
  }

  void disablePartition() {
    String instanceName = _manager.getInstanceName();
    ResourceId resourceId = _message.getResourceId();
    PartitionId partitionId = _message.getPartitionId();
    String clusterName = _manager.getClusterName();
    HelixAdmin admin = _manager.getClusterManagmentTool();
    admin.enablePartition(false, clusterName, instanceName, resourceId.stringify(),
        Arrays.asList(partitionId.stringify()));
    logger.info("error in transit from ERROR to " + _message.getTypedToState() + " for partition: "
        + partitionId + ". disable it on " + instanceName);

  }

  @Override
  public HelixTaskResult handleMessage() {
    NotificationContext context = _notificationContext;
    Message message = _message;

    synchronized (_stateModel) {
      HelixTaskResult taskResult = new HelixTaskResult();
      HelixManager manager = context.getManager();
      HelixDataAccessor accessor = manager.getHelixDataAccessor();

      _statusUpdateUtil.logInfo(message, HelixStateTransitionHandler.class,
          "Message handling task begin execute", accessor);
      message.setExecuteStartTimeStamp(new Date().getTime());

      try {
        preHandleMessage();
        invoke(accessor, context, taskResult, message);
      } catch (HelixStateMismatchException e) {
        // Simply log error and return from here if State mismatch.
        // The current state of the state model is intact.
        taskResult.setSuccess(false);
        taskResult.setMessage(e.toString());
        taskResult.setException(e);
      } catch (Exception e) {
        String errorMessage =
            "Exception while executing a state transition task " + message.getPartitionId();
        logger.error(errorMessage, e);
        if (e.getCause() != null && e.getCause() instanceof InterruptedException) {
          e = (InterruptedException) e.getCause();
        }
        _statusUpdateUtil.logError(message, HelixStateTransitionHandler.class, e, errorMessage,
            accessor);
        taskResult.setSuccess(false);
        taskResult.setMessage(e.toString());
        taskResult.setException(e);
        taskResult.setInterrupted(e instanceof InterruptedException);
      }

      // add task result to context for postHandling
      context.add(MapKey.HELIX_TASK_RESULT.toString(), taskResult);
      postHandleMessage();

      return taskResult;
    }
  }

  private void invoke(HelixDataAccessor accessor, NotificationContext context,
      HelixTaskResult taskResult, Message message) throws IllegalAccessException,
      InvocationTargetException, InterruptedException {
    _statusUpdateUtil.logInfo(message, HelixStateTransitionHandler.class,
        "Message handling invoking", accessor);

    // by default, we invoke state transition function in state model
    Method methodToInvoke = null;
    State fromState = message.getTypedFromState();
    State toState = message.getTypedToState();
    methodToInvoke =
        _transitionMethodFinder.getMethodForTransition(_stateModel.getClass(),
            fromState.toString(), toState.toString(), new Class[] {
                Message.class, NotificationContext.class
            });
    if (methodToInvoke != null) {
      logger.info(String.format(
          "Instance %s, partition %s received state transition from %s to %s on session %s.",
          message.getTgtName(), message.getPartitionName(), message.getFromState(),
          message.getToState(), message.getTgtSessionId()));
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
      String errorMessage =
          "Unable to find method for transition from " + fromState + " to " + toState + " in "
              + _stateModel.getClass();
      logger.error(errorMessage);
      taskResult.setSuccess(false);

      _statusUpdateUtil
          .logError(message, HelixStateTransitionHandler.class, errorMessage, accessor);
    }
  }

  @Override
  public void onError(Exception e, ErrorCode code, ErrorType type) {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    String instanceName = _manager.getInstanceName();
    ResourceId resourceId = _message.getResourceId();
    PartitionId partition = _message.getPartitionId();

    // All internal error has been processed already, so we can skip them
    if (type == ErrorType.INTERNAL) {
      logger.error("Skip internal error. errCode: " + code + ", errMsg: " + e.getMessage());
      return;
    }

    try {
      // set current state to ERROR for the partition
      // if the transition is not canceled, it should go into error state
      if (code == ErrorCode.ERROR) {
        CurrentState currentStateDelta = new CurrentState(resourceId.stringify());
        currentStateDelta.setState(partition, State.from(HelixDefinedState.ERROR.toString()));
        _stateModel.updateState(HelixDefinedState.ERROR.toString());

        // if transit from ERROR state, disable the partition
        if (_message.getTypedFromState().toString()
            .equalsIgnoreCase(HelixDefinedState.ERROR.toString())) {
          disablePartition();
        }
        accessor.updateProperty(keyBuilder.currentState(instanceName, _message
            .getTypedTgtSessionId().stringify(), resourceId.stringify()), currentStateDelta);
      }
    } finally {
      StateTransitionError error = new StateTransitionError(type, code, e);
      _stateModel.rollbackOnError(_message, _notificationContext, error);
    }

  }

  @Override
  public void onTimeout() {
    _isTimeout = true;
  }
};
