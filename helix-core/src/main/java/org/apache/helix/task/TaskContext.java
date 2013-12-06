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
 * Provides a typed interface to the context information stored by {@link TaskRebalancer} in the
 * Helix property store.
 */
public class TaskContext extends HelixProperty {
  public static final String START_TIME = "START_TIME";
  public static final String PARTITION_STATE = "STATE";
  public static final String NUM_ATTEMPTS = "NUM_ATTEMPTS";
  public static final String FINISH_TIME = "FINISH_TIME";

  public TaskContext(ZNRecord record) {
    super(record);
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

  public void setPartitionState(int p, TaskPartitionState s) {
    String pStr = String.valueOf(p);
    Map<String, String> map = _record.getMapField(pStr);
    if (map == null) {
      map = new TreeMap<String, String>();
      _record.setMapField(pStr, map);
    }
    map.put(PARTITION_STATE, s.name());
  }

  public TaskPartitionState getPartitionState(int p) {
    Map<String, String> map = _record.getMapField(String.valueOf(p));
    if (map == null) {
      return null;
    }

    String str = map.get(PARTITION_STATE);
    if (str != null) {
      return TaskPartitionState.valueOf(str);
    } else {
      return null;
    }
  }

  public void setPartitionNumAttempts(int p, int n) {
    String pStr = String.valueOf(p);
    Map<String, String> map = _record.getMapField(pStr);
    if (map == null) {
      map = new TreeMap<String, String>();
      _record.setMapField(pStr, map);
    }
    map.put(NUM_ATTEMPTS, String.valueOf(n));
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
    Map<String, String> map = _record.getMapField(String.valueOf(p));
    if (map == null) {
      return -1;
    }

    String nStr = map.get(NUM_ATTEMPTS);
    if (nStr == null) {
      return -1;
    }

    return Integer.parseInt(nStr);
  }

  public void setPartitionFinishTime(int p, long t) {
    String pStr = String.valueOf(p);
    Map<String, String> map = _record.getMapField(pStr);
    if (map == null) {
      map = new TreeMap<String, String>();
      _record.setMapField(pStr, map);
    }
    map.put(FINISH_TIME, String.valueOf(t));
  }

  public long getPartitionFinishTime(int p) {
    Map<String, String> map = _record.getMapField(String.valueOf(p));
    if (map == null) {
      return -1;
    }

    String tStr = map.get(FINISH_TIME);
    if (tStr == null) {
      return -1;
    }

    return Long.parseLong(tStr);
  }
}
