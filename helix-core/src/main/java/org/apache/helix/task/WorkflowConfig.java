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
  private JobDag _jobDag;
  private TargetState _targetState;
  private long _expiry;

  private WorkflowConfig(JobDag jobDag, TargetState targetState, long expiry) {
    _jobDag = jobDag;
    _targetState = targetState;
    _expiry = expiry;
  }

  public JobDag getJobDag() {
    return _jobDag;
  }

  public TargetState getTargetState() {
    return _targetState;
  }

  public long getExpiry() {
    return _expiry;
  }

  public static class Builder {
    private JobDag _taskDag = JobDag.EMPTY_DAG;
    private TargetState _targetState = TargetState.START;
    private long _expiry = DEFAULT_EXPIRY;

    public Builder() {
      // Nothing to do
    }

    public WorkflowConfig build() {
      validate();

      return new WorkflowConfig(_taskDag, _targetState, _expiry);
    }

    public Builder setTaskDag(JobDag v) {
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
        b.setTaskDag(JobDag.fromJson(cfg.get(DAG)));
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
