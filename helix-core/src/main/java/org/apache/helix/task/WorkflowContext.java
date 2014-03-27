package org.apache.helix.task;

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
  public static final String TASK_STATES = "TASK_STATES";
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

  public void setTaskState(String taskResource, TaskState s) {
    Map<String, String> states = _record.getMapField(TASK_STATES);
    if (states == null) {
      states = new TreeMap<String, String>();
      _record.setMapField(TASK_STATES, states);
    }
    states.put(taskResource, s.name());
  }

  public TaskState getTaskState(String taskResource) {
    Map<String, String> states = _record.getMapField(TASK_STATES);
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
}
