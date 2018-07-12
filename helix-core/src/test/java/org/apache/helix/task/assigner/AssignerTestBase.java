package org.apache.helix.task.assigner;

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
import java.util.Map;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.TaskConfig;

/* package */ class AssignerTestBase {

  private static final String testClusterName = "testCluster";
  static final String testInstanceName = "testInstance";

  static final String[] testResourceTypes = new String[] {"Resource1", "Resource2", "Resource3"};
  static final String[] testResourceCapacity = new String[] {"20", "50", "100"};

  static final String[] testQuotaTypes = new String[] {"Type1", "Type2", "Type3"};
  static final String[] testQuotaRatio = new String[] {"50", "30", "20"};
  private static final String defaultQuotaRatio = "100";

  /* package */ LiveInstance createLiveInstance(String[] resourceTypes, String[] resourceCapacity) {
    return createLiveInstance(resourceTypes, resourceCapacity, testInstanceName);
  }

  /* package */ LiveInstance createLiveInstance(String[] resourceTypes, String[] resourceCapacity,
      String instancename) {
    LiveInstance li = new LiveInstance(instancename);
    if (resourceCapacity != null && resourceTypes != null) {
      Map<String, String> resMap = new HashMap<>();
      for (int i = 0; i < resourceCapacity.length; i++) {
        resMap.put(resourceTypes[i], resourceCapacity[i]);
      }
      li.setResourceCapacityMap(resMap);
    }
    return li;
  }

  /* package */ ClusterConfig createClusterConfig(String[] quotaTypes, String[] quotaRatio,
      boolean addDefaultQuota) {
    ClusterConfig clusterConfig = new ClusterConfig(testClusterName);
    if (quotaTypes != null && quotaRatio != null) {
      for (int i = 0; i < quotaTypes.length; i++) {
        clusterConfig.setTaskQuotaRatio(quotaTypes[i], quotaRatio[i]);
      }
    }
    if (addDefaultQuota) {
      clusterConfig.setTaskQuotaRatio(AssignableInstance.DEFAULT_QUOTA_TYPE, defaultQuotaRatio);
    }
    return clusterConfig;
  }
}