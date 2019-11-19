package org.apache.helix.util;

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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Error;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StatusUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class to create statusUpdates ZK records and error ZK records. These message
 * records are for diagnostics only, and they are stored on the "StatusUpdates" and
 * "errors" ZNodes in the zookeeper instances.
 */
public class StatusUpdateUtil {
  static Logger _logger = LoggerFactory.getLogger(StatusUpdateUtil.class);

  public static class Transition implements Comparable<Transition> {
    private final String _msgID;
    private final long _timeStamp;
    private final String _from;
    private final String _to;

    public Transition(String msgID, long timeStamp, String from, String to) {
      this._msgID = msgID;
      this._timeStamp = timeStamp;
      this._from = from;
      this._to = to;
    }

    @Override
    public int compareTo(Transition t) {
      if (_timeStamp < t._timeStamp)
        return -1;
      else if (_timeStamp > t._timeStamp)
        return 1;
      else
        return 0;
    }

    public boolean equals(Transition t) {
      return (_timeStamp == t._timeStamp && _from.equals(t._from) && _to.equals(t._to));
    }

    public String getFromState() {
      return _from;
    }

    public String getToState() {
      return _to;
    }

    public String getMsgID() {
      return _msgID;
    }

    @Override
    public String toString() {
      return _msgID + ":" + _timeStamp + ":" + _from + "->" + _to;
    }
  }

  public static enum TaskStatus {
    UNKNOWN,
    NEW,
    SCHEDULED,
    INVOKING,
    COMPLETED,
    FAILED
  }

  public static class StatusUpdateContents {
    private final List<Transition> _transitions;
    private final Map<String, TaskStatus> _taskMessages;

    private StatusUpdateContents(List<Transition> transitions, Map<String, TaskStatus> taskMessages) {
      this._transitions = transitions;
      this._taskMessages = taskMessages;
    }

    public static StatusUpdateContents getStatusUpdateContents(HelixDataAccessor accessor,
        String instance, String resourceGroup, String partition) {
      return getStatusUpdateContents(accessor, instance, resourceGroup, null, partition);
    }

    // TODO: We should build a map and return the key instead of searching
    // everytime
    // for an (instance, resourceGroup, session, partition) tuple.
    // But such a map is very similar to what exists in ZNRecord
    // passing null for sessionID results in searching across all sessions
    public static StatusUpdateContents getStatusUpdateContents(HelixDataAccessor accessor,
        String instance, String resourceGroup, String sessionID, String partition) {
      Builder keyBuilder = accessor.keyBuilder();

      List<ZNRecord> instances =
          HelixProperty.convertToList(accessor.getChildValues(keyBuilder.instanceConfigs()));
      List<ZNRecord> partitionRecords = new ArrayList<ZNRecord>();
      for (ZNRecord znRecord : instances) {
        String instanceName = znRecord.getId();
        if (!instanceName.equals(instance)) {
          continue;
        }

        List<String> sessions = accessor.getChildNames(keyBuilder.sessions(instanceName));
        for (String session : sessions) {
          if (sessionID != null && !session.equals(sessionID)) {
            continue;
          }

          List<String> resourceGroups =
              accessor.getChildNames(keyBuilder.stateTransitionStatus(instanceName, session));
          for (String resourceGroupName : resourceGroups) {
            if (!resourceGroupName.equals(resourceGroup)) {
              continue;
            }

            List<String> partitionStrings =
                accessor.getChildNames(keyBuilder.stateTransitionStatus(instanceName, session,
                    resourceGroupName));

            for (String partitionString : partitionStrings) {
              ZNRecord partitionRecord =
                  accessor.getProperty(
                      keyBuilder.stateTransitionStatus(instanceName, session, resourceGroupName,
                          partitionString)).getRecord();
              if (!partitionString.equals(partition)) {
                continue;
              }
              partitionRecords.add(partitionRecord);
            }
          }
        }
      }

      return new StatusUpdateContents(getSortedTransitions(partitionRecords),
          getTaskMessages(partitionRecords));
    }

    public List<Transition> getTransitions() {
      return _transitions;
    }

    public Map<String, TaskStatus> getTaskMessages() {
      return _taskMessages;
    }

    // input: List<ZNRecord> corresponding to (instance, database,
    // partition) tuples across all sessions
    // return list of transitions sorted from earliest to latest
    private static List<Transition> getSortedTransitions(List<ZNRecord> partitionRecords) {
      List<Transition> transitions = new ArrayList<Transition>();
      for (ZNRecord partition : partitionRecords) {
        Map<String, Map<String, String>> mapFields = partition.getMapFields();
        for (String key : mapFields.keySet()) {
          if (key.startsWith("MESSAGE")) {
            Map<String, String> m = mapFields.get(key);
            long createTimeStamp = 0;
            try {
              createTimeStamp = Long.parseLong(m.get("CREATE_TIMESTAMP"));
            } catch (Exception e) {
            }
            transitions.add(new Transition(m.get("MSG_ID"), createTimeStamp, m.get("FROM_STATE"), m
                .get("TO_STATE")));
          }
        }
      }
      Collections.sort(transitions);
      return transitions;
    }

    private static Map<String, TaskStatus> getTaskMessages(List<ZNRecord> partitionRecords) {
      Map<String, TaskStatus> taskMessages = new HashMap<String, TaskStatus>();
      for (ZNRecord partition : partitionRecords) {
        Map<String, Map<String, String>> mapFields = partition.getMapFields();
        // iterate over the task status updates in the order they occurred
        // so that the last status can be recorded
        for (String key : mapFields.keySet()) {
          if (key.contains("STATE_TRANSITION")) {
            Map<String, String> m = mapFields.get(key);
            String id = m.get("MSG_ID");
            String statusString = m.get("AdditionalInfo");
            TaskStatus status = TaskStatus.UNKNOWN;
            if (statusString.contains("scheduled"))
              status = TaskStatus.SCHEDULED;
            else if (statusString.contains("invoking"))
              status = TaskStatus.INVOKING;
            else if (statusString.contains("completed"))
              status = TaskStatus.COMPLETED;

            taskMessages.put(id, status);
          }
        }
      }
      return taskMessages;
    }
  }

  public enum Level {
    HELIX_ERROR,
    HELIX_WARNING,
    HELIX_INFO
  }

  /**
   * Creates an empty ZNRecord as the statusUpdate/error record
   * @param id
   */
  public ZNRecord createEmptyStatusUpdateRecord(String id) {
    return new ZNRecord(id);
  }

  /**
   * Create a ZNRecord for a message, which stores the content of the message (stored in
   * simple fields) into the ZNRecord mapFields. In this way, the message update can be
   * merged with the previous status update record in the zookeeper. See ZNRecord.merge()
   * for more details.
   */
  ZNRecord createMessageLogRecord(Message message) {
    ZNRecord result = new ZNRecord(getStatusUpdateRecordName(message));
    String mapFieldKey = "MESSAGE " + message.getMsgId();
    result.setMapField(mapFieldKey, new TreeMap<String, String>());

    // Store all the simple fields of the message in the new ZNRecord's map
    // field.
    for (String simpleFieldKey : message.getRecord().getSimpleFields().keySet()) {
      result.getMapField(mapFieldKey).put(simpleFieldKey,
          message.getRecord().getSimpleField(simpleFieldKey));
    }
    if (message.getResultMap() != null) {
      result.setMapField("MessageResult", message.getResultMap());
    }
    return result;
  }

  Map<String, String> _recordedMessages = new ConcurrentHashMap<>();

  /**
   * Create a statusupdate that is related to a cluster manager message.
   * @param message
   *          the related cluster manager message
   * @param level
   *          the error level
   * @param classInfo
   *          class info about the class that reports the status update
   * @param additionalInfo
   *          info the additional debug information
   */
  public ZNRecord createMessageStatusUpdateRecord(Message message, Level level, Class classInfo,
      String additionalInfo) {
    ZNRecord result = createEmptyStatusUpdateRecord(getStatusUpdateRecordName(message));
    Map<String, String> contentMap = new TreeMap<String, String>();

    contentMap.put("Message state",
        (message.getMsgState() == null ? "NULL" : message.getMsgState().toString()));
    contentMap.put("AdditionalInfo", additionalInfo);
    contentMap.put("Class", classInfo.toString());
    contentMap.put("MSG_ID", message.getMsgId());

    result.setMapField(generateMapFieldId(level, getRecordIdForMessage(message)), contentMap);

    return result;
  }

  private String getRecordIdForMessage(Message message) {
    if (message.getMsgType().equals(MessageType.STATE_TRANSITION)) {
      return message.getPartitionName() + " Trans:" + message.getFromState().charAt(0) + "->"
          + message.getToState().charAt(0) + "  " + UUID.randomUUID().toString();
    } else {
      return message.getMsgType() + " " + UUID.randomUUID().toString();
    }
  }

  private String generateMapFieldId(Level level, String recordId) {
    DateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss.SSSSSS");
    String time = formatter.format(new Date());
    return String.format("%4s %26s ", level.toString(), time) + recordId;
  }

  @Deprecated
  public void logMessageStatusUpdateRecord(Message message, Level level, Class classInfo,
      String additionalInfo, HelixDataAccessor accessor) {
    try {
      ZNRecord record = createMessageStatusUpdateRecord(message, level, classInfo, additionalInfo);
      publishStatusUpdateRecord(record, message, level, accessor,
          message.getTgtName().equalsIgnoreCase(InstanceType.CONTROLLER.name()));
    } catch (Exception e) {
      _logger.error("Exception while logging status update", e);
    }
  }

  /**
   * Create a statusupdate that is related to a cluster manager message, then record it to
   * the zookeeper store.
   * @param message
   *          the related cluster manager message
   * @param level
   *          the error level
   * @param classInfo
   *          class info about the class that reports the status update
   * @param additionalInfo
   *          info the additional debug information
   * @param manager
   *          the HelixManager that writes the status update to zookeeper
   */
  public void logMessageStatusUpdateRecord(Message message, Level level, Class classInfo,
      String additionalInfo, HelixManager manager) {
    try {
      ZNRecord record = createMessageStatusUpdateRecord(message, level, classInfo, additionalInfo);
      publishStatusUpdateRecord(record, message, level, manager.getHelixDataAccessor(),
          manager.getInstanceType().equals(InstanceType.CONTROLLER) || manager.getInstanceType()
              .equals(InstanceType.CONTROLLER_PARTICIPANT));
    } catch (Exception e) {
      _logger.error("Exception while logging status update", e);
    }
  }

  public enum ErrorType {
    RebalanceResourceFailure,
  }

  public void logError(ErrorType errorType, Class classInfo, String additionalInfo, HelixManager helixManager) {
    if (helixManager != null) {
      logError(errorType, "ErrorInfo", helixManager.getInstanceName(), helixManager.getSessionId(), additionalInfo,
          classInfo, helixManager.getHelixDataAccessor(),
          helixManager.getInstanceType().equals(InstanceType.CONTROLLER) || helixManager.getInstanceType()
              .equals(InstanceType.CONTROLLER_PARTICIPANT));
    } else {
      _logger.error("Exception while logging error. HelixManager is null.");
    }
  }

  private void logError(ErrorType errorType, String updateKey, String instanceName,
      String sessionId, String additionalInfo, Class classInfo, HelixDataAccessor accessor,
      boolean isController) {
    try {
      ZNRecord record = createEmptyStatusUpdateRecord(sessionId + "__" + instanceName);

      Map<String, String> contentMap = new TreeMap<>();
      contentMap.put("AdditionalInfo", additionalInfo);
      contentMap.put("Class", classInfo.toString());
      contentMap.put("SessionId", sessionId);

      record.setMapField(generateMapFieldId(Level.HELIX_ERROR, updateKey), contentMap);

      publishErrorRecord(record, instanceName, errorType.name(), updateKey, sessionId, accessor,
          isController);
    } catch (Exception e) {
      _logger.error("Exception while logging error", e);
    }
  }

  public void logError(Message message, Class classInfo, String additionalInfo, HelixManager manager) {
    logMessageStatusUpdateRecord(message, Level.HELIX_ERROR, classInfo, additionalInfo, manager);
  }

  public void logError(Message message, Class classInfo, Exception e, String additionalInfo,
      HelixManager manager) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    logMessageStatusUpdateRecord(message, Level.HELIX_ERROR, classInfo,
        additionalInfo + sw.toString(), manager);
  }

  public void logInfo(Message message, Class classInfo, String additionalInfo,
      HelixManager manager) {
    logMessageStatusUpdateRecord(message, Level.HELIX_INFO, classInfo, additionalInfo, manager);
  }

  public void logWarning(Message message, Class classInfo, String additionalInfo,
      HelixManager manager) {
    logMessageStatusUpdateRecord(message, Level.HELIX_WARNING, classInfo, additionalInfo, manager);
  }

  @Deprecated
  public void logError(Message message, Class classInfo, String additionalInfo,
      HelixDataAccessor accessor) {
    logMessageStatusUpdateRecord(message, Level.HELIX_ERROR, classInfo, additionalInfo, accessor);
  }

  @Deprecated
  public void logError(Message message, Class classInfo, Exception e, String additionalInfo,
      HelixDataAccessor accessor) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    logMessageStatusUpdateRecord(message, Level.HELIX_ERROR, classInfo,
        additionalInfo + sw.toString(), accessor);
  }

  @Deprecated
  public void logInfo(Message message, Class classInfo, String additionalInfo,
      HelixDataAccessor accessor) {
    logMessageStatusUpdateRecord(message, Level.HELIX_INFO, classInfo, additionalInfo, accessor);
  }

  @Deprecated
  public void logWarning(Message message, Class classInfo, String additionalInfo,
      HelixDataAccessor accessor) {
    logMessageStatusUpdateRecord(message, Level.HELIX_WARNING, classInfo, additionalInfo, accessor);
  }

  private String getStatusUpdateKey(Message message) {
    if (message.getMsgType().equalsIgnoreCase(MessageType.STATE_TRANSITION.name())) {
      return message.getPartitionName();
    }
    return message.getMsgId();
  }

  /**
   * Generate the sub-path under STATUSUPDATE or ERROR path for a status update
   */
  String getStatusUpdateSubPath(Message message) {
    if (message.getMsgType().equalsIgnoreCase(MessageType.STATE_TRANSITION.name())) {
      return message.getResourceName();
    }
    return message.getMsgType();
  }

  String getStatusUpdateRecordName(Message message) {
    if (message.getMsgType().equalsIgnoreCase(MessageType.STATE_TRANSITION.name())) {
      return message.getTgtSessionId() + "__" + message.getResourceName();
    }
    return message.getMsgId();
  }

  /**
   * Write a status update record to zookeeper to the zookeeper store.
   * @param record
   *          the status update record
   * @param message
   *          the message to be logged
   * @param level
   *          the error level of the message update
   * @param accessor
   *          the zookeeper data accessor that writes the status update to zookeeper
   * @param isController
   *          if the update is for a controller instance or not
   */
  void publishStatusUpdateRecord(ZNRecord record, Message message, Level level,
      HelixDataAccessor accessor, boolean isController) {
    String instanceName = message.getTgtName();
    String statusUpdateSubPath = getStatusUpdateSubPath(message);
    String statusUpdateKey = getStatusUpdateKey(message);
    String sessionId = message.getExecutionSessionId();
    if (sessionId == null) {
      sessionId = message.getTgtSessionId();
    }
    if (sessionId == null) {
      sessionId = "*";
    }

    Builder keyBuilder = accessor.keyBuilder();
    if (!_recordedMessages.containsKey(message.getMsgId())) {
      if (isController) {
        accessor
            .updateProperty(keyBuilder.controllerTaskStatus(statusUpdateSubPath, statusUpdateKey),
                new StatusUpdate(createMessageLogRecord(message)));

      } else {

        PropertyKey propertyKey =
            keyBuilder.stateTransitionStatus(instanceName, sessionId, statusUpdateSubPath,
                statusUpdateKey);

        ZNRecord statusUpdateRecord = createMessageLogRecord(message);

        // For now write participant StatusUpdates to log4j.
        // we are using restlet as another data channel to report to controller.
        if (_logger.isTraceEnabled()) {
          _logger.trace("StatusUpdate path:" + propertyKey.getPath() + ", updates:"
              + statusUpdateRecord);
        }
        accessor.updateProperty(propertyKey, new StatusUpdate(statusUpdateRecord));

      }
      _recordedMessages.put(message.getMsgId(), message.getMsgId());
    }

    if (isController) {
      accessor.updateProperty(
          keyBuilder.controllerTaskStatus(statusUpdateSubPath, statusUpdateKey), new StatusUpdate(
              record));
    } else {

      PropertyKey propertyKey =
          keyBuilder.stateTransitionStatus(instanceName, sessionId, statusUpdateSubPath,
              statusUpdateKey);
      // For now write participant StatusUpdates to log4j.
      // we are using restlet as another data channel to report to controller.
      if (_logger.isTraceEnabled()) {
        _logger.trace("StatusUpdate path:" + propertyKey.getPath() + ", updates:" + record);
      }
      accessor.updateProperty(propertyKey, new StatusUpdate(record));
    }

    // If the error level is ERROR, also write the record to "ERROR" ZNode
    if (Level.HELIX_ERROR == level) {
      publishErrorRecord(record, instanceName, statusUpdateSubPath, statusUpdateKey, sessionId,
          accessor, isController);
    }
  }

  /**
   * Write an error record to zookeeper to the zookeeper store.
   * @param record
   *          the status update record
   * @param instanceName
   *          the instance name
   * @param updateSubPath
   *          the error update sub path
   * @param updateKey
   *          the error update key
   * @param sessionId
   *          the session id
   * @param accessor
   *          the zookeeper data accessor that writes the status update to zookeeper
   * @param isController
   *          if the error log is for a controller instance or not
   */
  void publishErrorRecord(ZNRecord record, String instanceName, String updateSubPath,
      String updateKey, String sessionId, HelixDataAccessor accessor, boolean isController) {
    Builder keyBuilder = accessor.keyBuilder();
    if (isController) {
      // TODO need to fix: ERRORS_CONTROLLER doesn't have a form of
      // ../{sessionId}/{subPath}
      accessor.setProperty(keyBuilder.controllerTaskError(updateSubPath), new Error(record));
    } else {
      accessor.updateProperty(keyBuilder.stateTransitionError(instanceName, sessionId,
          updateSubPath, updateKey), new Error(record));
    }
  }
}
