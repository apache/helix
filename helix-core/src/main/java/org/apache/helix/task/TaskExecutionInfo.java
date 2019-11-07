package org.apache.helix.task;

import java.io.IOException;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

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
