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

import java.util.Set;

import org.apache.helix.HelixManager;

public class AnalyticsTaskFactory implements TaskFactory {

  @Override
  public Task createTask(String id, Set<String> parentIds, HelixManager helixManager,
      TaskResultStore taskResultStore) {
    if (id.equalsIgnoreCase("filterImps")) {
      return new FilterTask(id, parentIds, helixManager, taskResultStore, FilterTask.IMPRESSIONS);
    } else if (id.equalsIgnoreCase("filterClicks")) {
      return new FilterTask(id, parentIds, helixManager, taskResultStore, FilterTask.CLICKS);
    } else if (id.equalsIgnoreCase("impClickJoin")) {
      return new JoinTask(id, parentIds, helixManager, taskResultStore,
          FilterTask.FILTERED_IMPRESSIONS, FilterTask.FILTERED_CLICKS);
    } else if (id.equalsIgnoreCase("impCountsByGender")) {
      return new CountTask(id, parentIds, helixManager, taskResultStore,
          FilterTask.FILTERED_IMPRESSIONS, "gender");
    } else if (id.equalsIgnoreCase("impCountsByCountry")) {
      return new CountTask(id, parentIds, helixManager, taskResultStore,
          FilterTask.FILTERED_IMPRESSIONS, "country");
    } else if (id.equalsIgnoreCase("clickCountsByGender")) {
      return new CountTask(id, parentIds, helixManager, taskResultStore, JoinTask.JOINED_CLICKS,
          "gender");
    } else if (id.equalsIgnoreCase("clickCountsByCountry")) {
      return new CountTask(id, parentIds, helixManager, taskResultStore, JoinTask.JOINED_CLICKS,
          "country");
    } else if (id.equalsIgnoreCase("report")) {
      return new ReportTask(id, parentIds, helixManager, taskResultStore);
    }

    throw new IllegalArgumentException("Cannot create task for " + id);
  }

}
