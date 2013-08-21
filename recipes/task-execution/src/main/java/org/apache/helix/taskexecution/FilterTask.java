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

import java.util.List;
import java.util.Set;

import org.apache.helix.HelixManager;

public class FilterTask extends Task {
  public static final String IMPRESSIONS = "impressions_demo";
  public static final String FILTERED_IMPRESSIONS = "filtered_impressions_demo";
  public static final String CLICKS = "clicks_demo";
  public static final String FILTERED_CLICKS = "filtered_clicks_demo";

  private final String _dataSource;

  public FilterTask(String id, Set<String> parentIds, HelixManager helixManager,
      TaskResultStore resultStore, String dataSource) {
    super(id, parentIds, helixManager, resultStore);
    _dataSource = dataSource;
  }

  @Override
  protected void executeImpl(String resourceName, int numPartitions, int partitionNum)
      throws Exception {
    System.out.println("Executing filter task for " + resourceName + "_" + partitionNum + " for "
        + _dataSource);
    long len = resultStore.llen(_dataSource);
    long bucketSize = len / numPartitions;
    long start = partitionNum * bucketSize;
    long end = start + bucketSize - 1;
    List<String> events = resultStore.lrange(_dataSource, start, end);
    String outputList = (_dataSource.equals(IMPRESSIONS) ? FILTERED_IMPRESSIONS : FILTERED_CLICKS);
    for (String event : events) {
      if (!isFraudulent(event)) {
        resultStore.rpush(outputList, event);
      }
    }
  }

  private boolean isFraudulent(String event) {
    String[] fields = event.split(",");
    return fields[1].equals("true");
  }
}
