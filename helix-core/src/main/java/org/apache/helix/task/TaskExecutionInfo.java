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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskExecutionInfo {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public final static long TIMESTAMP_NOT_SET = -1L;
  private final String _jobName;
  private final Integer _taskPartitionIndex;
  private final TaskPartitionState _taskPartitionState;
  private final Long _startTimeStamp;

  @JsonCreator
  public TaskExecutionInfo(
      @JsonProperty("jobName") String job,
      @JsonProperty("taskPartitionIndex") Integer index,
      @JsonProperty("taskPartitionState") TaskPartitionState state,
      @JsonProperty("startTimeStamp") Long timeStamp) {
    _jobName = job;
    _taskPartitionIndex = index;
    _taskPartitionState = state;
    _startTimeStamp = timeStamp == null ? TIMESTAMP_NOT_SET : timeStamp;
  }

  public String getJobName() {
    return _jobName;
  }

  public Integer getTaskPartitionIndex() {
    return _taskPartitionIndex;
  }

  public TaskPartitionState getTaskPartitionState() {
    return _taskPartitionState;
  }

  public Long getStartTimeStamp() {
    return _startTimeStamp;
  }

  public String toJson() throws IOException {
    return OBJECT_MAPPER.writeValueAsString(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof TaskExecutionInfo)) {
      return false;
    }
    TaskExecutionInfo infoObj = (TaskExecutionInfo) obj;
    return nullOrEquals(getJobName(), infoObj.getJobName()) &&
        nullOrEquals(getTaskPartitionIndex(), infoObj.getTaskPartitionIndex()) &&
        nullOrEquals(getTaskPartitionState(), infoObj.getTaskPartitionState()) &&
        nullOrEquals(getStartTimeStamp(), infoObj.getStartTimeStamp());
  }

  private boolean nullOrEquals(Object o1, Object o2) {
    return (o1 == null && o2 == null) || (o1 != null && o2 != null && o1.equals(o2));
  }
}
