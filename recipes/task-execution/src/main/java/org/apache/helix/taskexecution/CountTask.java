package org.apache.helix.taskexecution;

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

import org.apache.helix.HelixManager;

public class CountTask extends Task {

  private final String _groupByCol;
  private final String _eventSource;

  public CountTask(String id, Set<String> parentIds, HelixManager helixManager,
      TaskResultStore resultStore, String eventSource, String groupByCol) {
    super(id, parentIds, helixManager, resultStore);
    _eventSource = eventSource;
    _groupByCol = groupByCol;
  }

  @Override
  protected void executeImpl(String resourceName, int numPartitions, int partitionNum)
      throws Exception {
    System.out.println("Running AggTask for " + resourceName + "_" + partitionNum + " for "
        + _eventSource + " " + _groupByCol);
    if (!(_eventSource.equals(FilterTask.FILTERED_IMPRESSIONS) || _eventSource
        .equals(JoinTask.JOINED_CLICKS))) {
      throw new RuntimeException("Unsupported event source:" + _eventSource);
    }

    long len = resultStore.llen(_eventSource);
    long bucketSize = len / numPartitions;
    long start = partitionNum * bucketSize;
    long end = start + bucketSize - 1;
    List<String> events = resultStore.lrange(_eventSource, start, end);
    Map<String, Integer> counts = new HashMap<String, Integer>();
    for (String event : events) {
      String[] fields = event.split(",");
      if (_groupByCol.equals("gender")) {
        String gender =
            (_eventSource.equals(FilterTask.FILTERED_IMPRESSIONS) ? fields[3] : fields[4]);
        incrementGroupCount(gender, counts);
      } else if (_groupByCol.equals("country")) {
        String country =
            (_eventSource.equals(FilterTask.FILTERED_IMPRESSIONS) ? fields[2] : fields[3]);
        incrementGroupCount(country, counts);
      }
    }

    for (String key : counts.keySet()) {
      resultStore.hincrBy(_eventSource + "_" + _groupByCol + "_counts", key, counts.get(key));
    }
  }

  private void incrementGroupCount(String group, Map<String, Integer> counts) {
    int count = (counts.containsKey(group) ? counts.get(group) : 0);
    counts.put(group, count + 1);
  }

}
