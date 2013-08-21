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

import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;

public class ReportTask extends Task {

  public ReportTask(String id, Set<String> parentIds, HelixManager helixManager,
      TaskResultStore resultStore) {
    super(id, parentIds, helixManager, resultStore);
  }

  @Override
  protected void executeImpl(String resourceName, int numPartitions, int partitionNum)
      throws Exception {
    System.out.println("Running reports task");

    System.out.println("Impression counts per country");
    printCounts(FilterTask.FILTERED_IMPRESSIONS + "_country_counts");

    System.out.println("Click counts per country");
    printCounts(JoinTask.JOINED_CLICKS + "_country_counts");

    System.out.println("Impression counts per gender");
    printCounts(FilterTask.FILTERED_IMPRESSIONS + "_gender_counts");

    System.out.println("Click counts per gender");
    printCounts(JoinTask.JOINED_CLICKS + "_gender_counts");
  }

  private void printCounts(String tableName) throws Exception {
    Map<String, String> counts = resultStore.hgetAll(tableName);
    System.out.println(counts);
  }

}
