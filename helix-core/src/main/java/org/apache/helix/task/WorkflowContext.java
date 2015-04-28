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

import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

/**
 * Typed interface to the workflow context information stored by {@link TaskRebalancer} in the Helix
 * property store
 */
public class WorkflowContext extends HelixProperty {
  public static final String WORKFLOW_STATE = "STATE";
  public static final String START_TIME = "START_TIME";
  public static final String FINISH_TIME = "FINISH_TIME";
  public static final String JOB_STATES = "JOB_STATES";
  public static final String LAST_SCHEDULED_WORKFLOW = "LAST_SCHEDULED_WORKFLOW";
  public static final int UNFINISHED = -1;

  public WorkflowContext(ZNRecord record) {
    super(record);
  }

  public void setWorkflowState(TaskState s) {
    if (_record.getSimpleField(WORKFLOW_STATE) == null) {
      _record.setSimpleField(WORKFLOW_STATE, s.name());
    } else if (!_record.getSimpleField(WORKFLOW_STATE).equals(TaskState.FAILED.name())
        && !_record.getSimpleField(WORKFLOW_STATE).equals(TaskState.COMPLETED.name())) {
      _record.setSimpleField(WORKFLOW_STATE, s.name());
    }
  }

  public TaskState getWorkflowState() {
    String s = _record.getSimpleField(WORKFLOW_STATE);
    if (s == null) {
      return null;
    }

    return TaskState.valueOf(s);
  }

  public void setJobState(String jobResource, TaskState s) {
    Map<String, String> states = _record.getMapField(JOB_STATES);
    if (states == null) {
      states = new TreeMap<String, String>();
      _record.setMapField(JOB_STATES, states);
    }
    states.put(jobResource, s.name());
  }

  public TaskState getJobState(String jobResource) {
    Map<String, String> states = _record.getMapField(JOB_STATES);
    if (states == null) {
      return null;
    }

    String s = states.get(jobResource);
    if (s == null) {
      return null;
    }

    return TaskState.valueOf(s);
  }

  public void setStartTime(long t) {
    _record.setSimpleField(START_TIME, String.valueOf(t));
  }

  public long getStartTime() {
    String tStr = _record.getSimpleField(START_TIME);
    if (tStr == null) {
      return -1;
    }

    return Long.parseLong(tStr);
  }

  public void setFinishTime(long t) {
    _record.setSimpleField(FINISH_TIME, String.valueOf(t));
  }

  public long getFinishTime() {
    String tStr = _record.getSimpleField(FINISH_TIME);
    if (tStr == null) {
      return UNFINISHED;
    }

    return Long.parseLong(tStr);
  }

  public void setLastScheduledSingleWorkflow(String wf) {
    _record.setSimpleField(LAST_SCHEDULED_WORKFLOW, wf);
  }

  public String getLastScheduledSingleWorkflow() {
    return _record.getSimpleField(LAST_SCHEDULED_WORKFLOW);
  }
}
