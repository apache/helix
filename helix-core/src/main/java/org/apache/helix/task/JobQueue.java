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
 * A named queue to which jobs can be added
 */
public class JobQueue extends WorkflowConfig {
  /* Config fields */
  public static final String CAPACITY = "CAPACITY";

  private final String _name;
  private final int _capacity;

  private JobQueue(String name, int capacity, WorkflowConfig config) {
    super(config.getJobDag(), config.getTargetState(), config.getExpiry(), config.isTerminable(),
        config.getScheduleConfig());
    _name = name;
    _capacity = capacity;
  }

  /**
   * Get the name of this queue
   * @return queue name
   */
  public String getName() {
    return _name;
  }

  /**
   * Determine the number of jobs that this queue can accept before rejecting further jobs
   * @return queue capacity
   */
  public int getCapacity() {
    return _capacity;
  }

  @Override
  public Map<String, String> getResourceConfigMap() throws Exception {
    Map<String, String> cfgMap = super.getResourceConfigMap();
    cfgMap.put(CAPACITY, String.valueOf(_capacity));
    return cfgMap;
  }

  /** Supports creation of a single empty queue */
  public static class Builder {
    private WorkflowConfig.Builder _builder;
    private final String _name;
    private int _capacity = Integer.MAX_VALUE;

    public Builder(String name) {
      _builder = new WorkflowConfig.Builder();
      _name = name;
    }

    public Builder expiry(long expiry) {
      _builder.setExpiry(expiry);
      return this;
    }

    public Builder capacity(int capacity) {
      _capacity = capacity;
      return this;
    }

    public Builder fromMap(Map<String, String> cfg) {
      _builder = WorkflowConfig.Builder.fromMap(cfg);
      if (cfg.containsKey(CAPACITY)) {
        _capacity = Integer.parseInt(cfg.get(CAPACITY));
      }
      return this;
    }

    public JobQueue build() {
      _builder.setTerminable(false);
      WorkflowConfig workflowConfig = _builder.build();
      return new JobQueue(_name, _capacity, workflowConfig);
    }
  }
}
