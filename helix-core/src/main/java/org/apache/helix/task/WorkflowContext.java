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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;

/**
 * Typed interface to the workflow context information stored by {@link TaskRebalancer} in the Helix
 * property store
 */
public class WorkflowContext extends HelixProperty {
  private static final Logger LOG = Logger.getLogger(WorkflowContext.class);

  protected enum WorkflowContextProperties {
    STATE,
    START_TIME,
    FINISH_TIME,
    JOB_STATES,
    LAST_SCHEDULED_WORKFLOW,
    SCHEDULED_WORKFLOWS,
    LAST_PURGE_TIME,
    StartTime // TODO this should be named JOB_SCHEDULED_START_TIME, it's not the actual start time of the job
    }

  public static final int UNSTARTED = -1;
  public static final int UNFINISHED = -1;

  public WorkflowContext(ZNRecord record) {
    super(record);
  }

  public void setWorkflowState(TaskState s) {
    String workflowState = _record.getSimpleField(WorkflowContextProperties.STATE.name());
    if (workflowState == null) {
      _record.setSimpleField(WorkflowContextProperties.STATE.name(), s.name());
    } else if (!workflowState.equals(TaskState.FAILED.name()) && !workflowState
        .equals(TaskState.COMPLETED.name())) {
      _record.setSimpleField(WorkflowContextProperties.STATE.name(), s.name());
    }
  }

  public TaskState getWorkflowState() {
    String state = _record.getSimpleField(WorkflowContextProperties.STATE.name());
    if (state == null) {
      return TaskState.NOT_STARTED;
    }

    return TaskState.valueOf(state);
  }

  public void setJobState(String job, TaskState s) {
    Map<String, String> states = _record.getMapField(WorkflowContextProperties.JOB_STATES.name());
    if (states == null) {
      states = new TreeMap<String, String>();
      _record.setMapField(WorkflowContextProperties.JOB_STATES.name(), states);
    }
    states.put(job, s.name());
  }

  protected void removeJobStates(Set<String> jobs) {
    Map<String, String> states = _record.getMapField(WorkflowContextProperties.JOB_STATES.name());
    if (states != null) {
      states.keySet().removeAll(jobs);
      _record.setMapField(WorkflowContextProperties.JOB_STATES.name(), states);
    }
  }

  public TaskState getJobState(String job) {
    Map<String, String> states = _record.getMapField(WorkflowContextProperties.JOB_STATES.name());
    if (states == null) {
      return null;
    }

    String s = states.get(job);
    if (s == null) {
      return null;
    }

    return TaskState.valueOf(s);
  }

  protected void setJobStartTime(String job, long time) {
    Map<String, String> startTimes =
        _record.getMapField(WorkflowContextProperties.StartTime.name());
    if (startTimes == null) {
      startTimes = new HashMap<String, String>();
      _record.setMapField(WorkflowContextProperties.StartTime.name(), startTimes);
    }
    startTimes.put(job, String.valueOf(time));
  }

  protected void removeJobStartTime(Set<String> jobs) {
    Map<String, String> startTimes =
        _record.getMapField(WorkflowContextProperties.StartTime.name());
    if (startTimes != null) {
      startTimes.keySet().removeAll(jobs);
      _record.setMapField(WorkflowContextProperties.StartTime.name(), startTimes);
    }
  }

  public long getJobStartTime(String job) {
    Map<String, String> startTimes =
        _record.getMapField(WorkflowContextProperties.StartTime.name());
    if (startTimes == null || !startTimes.containsKey(job)) {
      return -1;
    }

    String t = startTimes.get(job);
    if (t == null) {
      return -1;
    }

    try {
      long ret = Long.valueOf(t);
      return ret;
    } catch (NumberFormatException e) {
      LOG.warn("Number error " + t + " for job start time of " + job);
      return -1;
    }
  }

  public Map<String, Long> getJobStartTimes() {
    Map<String, Long> startTimes = new HashMap<String, Long>();
    Map<String, String> startTimesMap =
        _record.getMapField(WorkflowContextProperties.StartTime.name());
    if (startTimesMap != null) {
      for (Map.Entry<String, String> time : startTimesMap.entrySet()) {
        startTimes.put(time.getKey(), Long.valueOf(time.getValue()));
      }
    }

    return startTimes;
  }

  public Map<String, TaskState> getJobStates() {
    Map<String, TaskState> jobStates = new HashMap<String, TaskState>();
    Map<String, String> stateFieldMap =
        _record.getMapField(WorkflowContextProperties.JOB_STATES.name());
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
      workflows = new ArrayList<String>();
      _record.setListField(WorkflowContextProperties.SCHEDULED_WORKFLOWS.name(), workflows);
    }
    workflows.add(workflow);
  }

  public String getLastScheduledSingleWorkflow() {
    return _record.getSimpleField(WorkflowContextProperties.LAST_SCHEDULED_WORKFLOW.name());
  }

  protected void setLastJobPurgeTime(long epochTime) {
    _record.setSimpleField(WorkflowContextProperties.LAST_PURGE_TIME.name(),
        String.valueOf(epochTime));
  }

  public long getLastJobPurgeTime() {
    return _record.getLongField(WorkflowContextProperties.LAST_PURGE_TIME.name(), -1);
  }

  public List<String> getScheduledWorkflows() {
    return _record.getListField(WorkflowContextProperties.SCHEDULED_WORKFLOWS.name());
  }
}
