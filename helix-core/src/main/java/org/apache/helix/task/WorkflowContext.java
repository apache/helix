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

import java.util.*;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

/**
 * Typed interface to the workflow context information stored by {@link TaskRebalancer} in the Helix
 * property store
 */
public class WorkflowContext extends HelixProperty {
  protected enum WorkflowContextProperties {
    STATE,
    START_TIME,
    FINISH_TIME,
    JOB_STATES,
    LAST_SCHEDULED_WORKFLOW,
    SCHEDULED_WORKFLOWS,
  }
  public static final int UNSTARTED = -1;
  public static final int UNFINISHED = -1;

  public WorkflowContext(ZNRecord record) {
    super(record);
  }

  public void setWorkflowState(TaskState s) {
    if (_record.getSimpleField(WorkflowContextProperties.STATE.name()) == null) {
      _record.setSimpleField(WorkflowContextProperties.STATE.name(), s.name());
    } else if (!_record.getSimpleField(WorkflowContextProperties.STATE.name())
        .equals(TaskState.FAILED.name()) && !_record
        .getSimpleField(WorkflowContextProperties.STATE.name())
        .equals(TaskState.COMPLETED.name())) {
      _record.setSimpleField(WorkflowContextProperties.STATE.name(), s.name());
    }
  }

  public TaskState getWorkflowState() {
    String s = _record.getSimpleField(WorkflowContextProperties.STATE.name());
    if (s == null) {
      return null;
    }

    return TaskState.valueOf(s);
  }

  public void setJobState(String jobResource, TaskState s) {
    Map<String, String> states = _record.getMapField(WorkflowContextProperties.JOB_STATES.name());
    if (states == null) {
      states = new TreeMap<>();
      _record.setMapField(WorkflowContextProperties.JOB_STATES.name(), states);
    }
    states.put(jobResource, s.name());
  }

  public TaskState getJobState(String jobResource) {
    Map<String, String> states = _record.getMapField(WorkflowContextProperties.JOB_STATES.name());
    if (states == null) {
      return null;
    }

    String s = states.get(jobResource);
    if (s == null) {
      return null;
    }

    return TaskState.valueOf(s);
  }

  public Map<String, TaskState> getJobStates() {
    Map<String, TaskState> jobStates = new HashMap<>();
    Map<String, String> stateFieldMap = _record.getMapField(WorkflowContextProperties.JOB_STATES.name());
    if (stateFieldMap != null) {
      for (Map.Entry<String, String> state : stateFieldMap.entrySet()) {
        jobStates.put(state.getKey(), TaskState.valueOf(state.getValue()));
      }
    }

    return jobStates;
  }

  public void setStartTime(long t) {
    _record.setSimpleField(WorkflowContextProperties.START_TIME.name(), String.valueOf(t));
  }

  public long getStartTime() {
    String tStr = _record.getSimpleField(WorkflowContextProperties.START_TIME.name());
    if (tStr == null) {
      return -1;
    }

    return Long.parseLong(tStr);
  }

  public void setFinishTime(long t) {
    _record.setSimpleField(WorkflowContextProperties.FINISH_TIME.name(), String.valueOf(t));
  }

  public long getFinishTime() {
    String tStr = _record.getSimpleField(WorkflowContextProperties.FINISH_TIME.name());
    if (tStr == null) {
      return UNFINISHED;
    }

    return Long.parseLong(tStr);
  }

  public void setLastScheduledSingleWorkflow(String workflow) {
    _record.setSimpleField(WorkflowContextProperties.LAST_SCHEDULED_WORKFLOW.name(), workflow);
    // Record scheduled workflow into the history list as well
    List<String> workflows = getScheduledWorkflows();
    if (workflows == null) {
      workflows = new ArrayList<>();
      _record.setListField(WorkflowContextProperties.SCHEDULED_WORKFLOWS.name(), workflows);
    }
    workflows.add(workflow);
  }

  public String getLastScheduledSingleWorkflow() {
    return _record.getSimpleField(WorkflowContextProperties.LAST_SCHEDULED_WORKFLOW.name());
  }

  public List<String> getScheduledWorkflows() {
    return _record.getListField(WorkflowContextProperties.SCHEDULED_WORKFLOWS.name());
  }

}
