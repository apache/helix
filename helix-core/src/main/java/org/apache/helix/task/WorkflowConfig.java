package org.apache.helix.task;

import java.util.Map;

/**
 * Provides a typed interface to workflow level configurations. Validates the configurations.
 */
public class WorkflowConfig {
  /* Config fields */
  public static final String DAG = "Dag";
  public static final String TARGET_STATE = "TargetState";
  public static final String EXPIRY = "Expiry";

  /* Default values */
  public static final long DEFAULT_EXPIRY = 24 * 60 * 60 * 1000;

  /* Member variables */
  private TaskDag _taskDag;
  private TargetState _targetState;
  private long _expiry;

  private WorkflowConfig(TaskDag taskDag, TargetState targetState, long expiry) {
    _taskDag = taskDag;
    _targetState = targetState;
    _expiry = expiry;
  }

  public TaskDag getTaskDag() {
    return _taskDag;
  }

  public TargetState getTargetState() {
    return _targetState;
  }

  public long getExpiry() {
    return _expiry;
  }

  public static class Builder {
    private TaskDag _taskDag = TaskDag.EMPTY_DAG;
    private TargetState _targetState = TargetState.START;
    private long _expiry = DEFAULT_EXPIRY;

    public Builder() {
      // Nothing to do
    }

    public WorkflowConfig build() {
      validate();

      return new WorkflowConfig(_taskDag, _targetState, _expiry);
    }

    public Builder setTaskDag(TaskDag v) {
      _taskDag = v;
      return this;
    }

    public Builder setExpiry(long v) {
      _expiry = v;
      return this;
    }

    public Builder setTargetState(TargetState v) {
      _targetState = v;
      return this;
    }

    public static Builder fromMap(Map<String, String> cfg) {
      Builder b = new Builder();

      if (cfg.containsKey(EXPIRY)) {
        b.setExpiry(Long.parseLong(cfg.get(EXPIRY)));
      }
      if (cfg.containsKey(DAG)) {
        b.setTaskDag(TaskDag.fromJson(cfg.get(DAG)));
      }
      if (cfg.containsKey(TARGET_STATE)) {
        b.setTargetState(TargetState.valueOf(cfg.get(TARGET_STATE)));
      }

      return b;
    }

    private void validate() {
      if (_expiry < 0) {
        throw new IllegalArgumentException(
            String.format("%s has invalid value %s", EXPIRY, _expiry));
      }
    }
  }

}
