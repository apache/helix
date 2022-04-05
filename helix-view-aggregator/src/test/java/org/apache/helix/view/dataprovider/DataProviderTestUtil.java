package org.apache.helix.view.dataprovider;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.helix.PropertyType;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.model.ClusterConfig;

public class DataProviderTestUtil {
  private static final int viewClusterRefreshPeriod = 10;
  private static final String testZkAddr = "localhost:1010";
  private static final List<PropertyType> testProperties =
      Arrays.asList(PropertyType.LIVEINSTANCES, PropertyType.EXTERNALVIEW, PropertyType.INSTANCES);

  public static ClusterConfig createDefaultViewClusterConfig(String viewClusterName,
      int numSourceCluster) {
    ClusterConfig config = new ClusterConfig(viewClusterName);
    config.setViewCluster();
    config.setViewClusterRefreshPeriod(viewClusterRefreshPeriod);
    List<ViewClusterSourceConfig> sourceConfigs = new ArrayList<>();
    for (int i = 0; i < numSourceCluster; i++) {
      String sourceClusterName = "cluster" + i;
      sourceConfigs.add(new ViewClusterSourceConfig(sourceClusterName, testZkAddr, testProperties));
    }
    config.setViewClusterSourceConfigs(sourceConfigs);
    return config;
  }
}
