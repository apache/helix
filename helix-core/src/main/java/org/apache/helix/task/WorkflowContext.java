package org.apache.helix.task;

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

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

import java.util.Map;
import java.util.TreeMap;

/**
 * Typed interface to the workflow context information stored by {@link TaskRebalancer} in the Helix
 * property store
 */
public class WorkflowContext extends HelixProperty {

  enum WorkflowContextEnum {
    WORKFLOW_STATE("STATE"),
    START_TIME("START_TIME"),
    FINISH_TIME("FINISH_TIME"),
    TASK_STATES("TASK_STATES");

    final String _value;

    private WorkflowContextEnum(String value) {
      _value = value;
    }

    public String value() {
      return _value;
    }
  }

  public static final int UNFINISHED = -1;

  public WorkflowContext(ZNRecord record) {
    super(record);
  }

  public void setWorkflowState(TaskState s) {
    if (_record.getSimpleField(WorkflowContextEnum.WORKFLOW_STATE.value()) == null) {
      _record.setSimpleField(WorkflowContextEnum.WORKFLOW_STATE.value(), s.name());
    } else if (!_record.getSimpleField(WorkflowContextEnum.WORKFLOW_STATE.value()).equals(
        TaskState.FAILED.name())
        && !_record.getSimpleField(WorkflowContextEnum.WORKFLOW_STATE.value()).equals(
            TaskState.COMPLETED.name())) {
      _record.setSimpleField(WorkflowContextEnum.WORKFLOW_STATE.value(), s.name());
    }
  }

  public TaskState getWorkflowState() {
    String s = _record.getSimpleField(WorkflowContextEnum.WORKFLOW_STATE.value());
    if (s == null) {
      return null;
    }

    return TaskState.valueOf(s);
  }

  public void setTaskState(String taskResource, TaskState s) {
    Map<String, String> states = _record.getMapField(WorkflowContextEnum.TASK_STATES.value());
    if (states == null) {
      states = new TreeMap<String, String>();
      _record.setMapField(WorkflowContextEnum.TASK_STATES.value(), states);
    }
    states.put(taskResource, s.name());
  }

  public TaskState getTaskState(String taskResource) {
    Map<String, String> states = _record.getMapField(WorkflowContextEnum.TASK_STATES.value());
    if (states == null) {
      return null;
    }

    String s = states.get(taskResource);
    if (s == null) {
      return null;
    }

    return TaskState.valueOf(s);
  }

  public void setStartTime(long t) {
    _record.setSimpleField(WorkflowContextEnum.START_TIME.value(), String.valueOf(t));
  }

  public long getStartTime() {
    String tStr = _record.getSimpleField(WorkflowContextEnum.START_TIME.value());
    if (tStr == null) {
      return -1;
    }

    return Long.parseLong(tStr);
  }

  public void setFinishTime(long t) {
    _record.setSimpleField(WorkflowContextEnum.FINISH_TIME.value(), String.valueOf(t));
  }

  public long getFinishTime() {
    String tStr = _record.getSimpleField(WorkflowContextEnum.FINISH_TIME.value());
    if (tStr == null) {
      return UNFINISHED;
    }

    return Long.parseLong(tStr);
  }
}
