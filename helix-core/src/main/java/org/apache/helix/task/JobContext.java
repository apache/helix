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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Provides a typed interface to the context information stored by {@link TaskRebalancer} in the
 * Helix property store.
 */
public class JobContext extends HelixProperty {
  private enum ContextProperties {
    START_TIME,
    STATE,
    NUM_ATTEMPTS,
    FINISH_TIME,
    TARGET,
    TASK_ID,
    ASSIGNED_PARTICIPANT,
    NEXT_RETRY_TIME
  }

  public JobContext(ZNRecord record) {
    super(record);
  }

  public void setStartTime(long t) {
    _record.setSimpleField(ContextProperties.START_TIME.toString(), String.valueOf(t));
  }

  public long getStartTime() {
    String tStr = _record.getSimpleField(ContextProperties.START_TIME.toString());
    if (tStr == null) {
      return -1;
    }
    return Long.parseLong(tStr);
  }

  public void setFinishTime(long t) {
    _record.setSimpleField(ContextProperties.FINISH_TIME.toString(), String.valueOf(t));
  }

  public long getFinishTime() {
    String tStr = _record.getSimpleField(ContextProperties.FINISH_TIME.toString());
    if (tStr == null) {
      return WorkflowContext.UNFINISHED;
    }
    return Long.parseLong(tStr);
  }

  public void setPartitionState(int p, TaskPartitionState s) {
    Map<String, String> map = getMapField(p);
    map.put(ContextProperties.STATE.toString(), s.name());
  }

  public TaskPartitionState getPartitionState(int p) {
    Map<String, String> map = getMapField(p);
    if (map == null) {
      return null;
    }
    String str = map.get(ContextProperties.STATE.toString());
    if (str != null) {
      return TaskPartitionState.valueOf(str);
    } else {
      return null;
    }
  }

  public void setPartitionNumAttempts(int p, int n) {
    Map<String, String> map = getMapField(p);
    map.put(ContextProperties.NUM_ATTEMPTS.toString(), String.valueOf(n));
  }

  public int incrementNumAttempts(int pId) {
    int n = this.getPartitionNumAttempts(pId);
    if (n < 0) {
      n = 0;
    }
    n += 1;
    this.setPartitionNumAttempts(pId, n);
    return n;
  }

  public int getPartitionNumAttempts(int p) {
    Map<String, String> map = getMapField(p);
    if (map == null) {
      return -1;
    }
    String nStr = map.get(ContextProperties.NUM_ATTEMPTS.toString());
    if (nStr == null) {
      return -1;
    }
    return Integer.parseInt(nStr);
  }

  public void setPartitionFinishTime(int p, long t) {
    Map<String, String> map = getMapField(p);
    map.put(ContextProperties.FINISH_TIME.toString(), String.valueOf(t));
  }

  public long getPartitionFinishTime(int p) {
    Map<String, String> map = getMapField(p);
    if (map == null) {
      return -1;
    }
    String tStr = map.get(ContextProperties.FINISH_TIME.toString());
    if (tStr == null) {
      return -1;
    }
    return Long.parseLong(tStr);
  }

  public void setPartitionTarget(int p, String targetPName) {
    Map<String, String> map = getMapField(p);
    map.put(ContextProperties.TARGET.toString(), targetPName);
  }

  public String getTargetForPartition(int p) {
    Map<String, String> map = getMapField(p);
    return (map != null) ? map.get(ContextProperties.TARGET.toString()) : null;
  }

  public Map<String, List<Integer>> getPartitionsByTarget() {
    Map<String, List<Integer>> result = Maps.newHashMap();
    for (Map.Entry<String, Map<String, String>> mapField : _record.getMapFields().entrySet()) {
      Integer pId = Integer.parseInt(mapField.getKey());
      Map<String, String> map = mapField.getValue();
      String target = map.get(ContextProperties.TARGET.toString());
      if (target != null) {
        List<Integer> partitions;
        if (!result.containsKey(target)) {
          partitions = Lists.newArrayList();
          result.put(target, partitions);
        } else {
          partitions = result.get(target);
        }
        partitions.add(pId);
      }
    }
    return result;
  }

  public Set<Integer> getPartitionSet() {
    Set<Integer> partitions = Sets.newHashSet();
    for (String pName : _record.getMapFields().keySet()) {
      partitions.add(Integer.valueOf(pName));
    }
    return partitions;
  }

  public void setTaskIdForPartition(int p, String taskId) {
    Map<String, String> map = getMapField(p);
    map.put(ContextProperties.TASK_ID.toString(), taskId);
  }

  public String getTaskIdForPartition(int p) {
    Map<String, String> map = getMapField(p);
    return (map != null) ? map.get(ContextProperties.TASK_ID.toString()) : null;
  }

  public Map<String, Integer> getTaskIdPartitionMap() {
    Map<String, Integer> partitionMap = new HashMap<String, Integer>();
    for (Map.Entry<String, Map<String, String>> mapField : _record.getMapFields().entrySet()) {
      Integer pId = Integer.parseInt(mapField.getKey());
      Map<String, String> map = mapField.getValue();
      if (map.containsKey(ContextProperties.TASK_ID.toString())) {
        partitionMap.put(map.get(ContextProperties.TASK_ID.toString()), pId);
      }
    }
    return partitionMap;
  }

  public void setAssignedParticipant(int p, String participantName) {
    Map<String, String> map = getMapField(p);
    map.put(ContextProperties.ASSIGNED_PARTICIPANT.toString(), participantName);
  }

  public String getAssignedParticipant(int p) {
    Map<String, String> map = getMapField(p);
    return (map != null) ? map.get(ContextProperties.ASSIGNED_PARTICIPANT.toString()) : null;
  }

  public void setNextRetryTime(int p, long t) {
    Map<String, String> map = getMapField(p);
    map.put(ContextProperties.NEXT_RETRY_TIME.toString(), String.valueOf(t));
  }

  public long getNextRetryTime(int p) {
    Map<String, String> map = getMapField(p);
    if (map == null) {
      return -1;
    }
    String tStr = map.get(ContextProperties.NEXT_RETRY_TIME.toString());
    if (tStr == null) {
      return -1;
    }
    return Long.parseLong(tStr);
  }

  public Map<String, String> getMapField(int p) {
    String pStr = String.valueOf(p);
    Map<String, String> map = _record.getMapField(pStr);
    if (map == null) {
      map = new TreeMap<String, String>();
      _record.setMapField(pStr, map);
    }
    return map;
  }
}
