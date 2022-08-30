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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.common.caches.BasicClusterDataCache;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;


public class ViewClusterDataCache extends BasicClusterDataCache {

  public ViewClusterDataCache(String viewClusterName) {
    super(viewClusterName);
  }

  /**
   * Update cache and return true if any local data is changed compared to before refresh.
   * @param dataAccessor the data accessor used to fetch data.
   * @return true if there is a change to local cache.
   */
  public boolean updateCache(HelixDataAccessor dataAccessor) {
    Map<String, LiveInstance> liveInstanceSnapshot = new HashMap<>(getLiveInstances());
    Map<String, ExternalView> externalViewSnapshot = new HashMap<>(getExternalViews());
    Map<String, InstanceConfig> instanceConfigSnapshot = new HashMap<>(getInstanceConfigMap());
    requireFullRefresh();
    refresh(dataAccessor);

    return !(liveInstanceSnapshot.equals(getLiveInstances())
        && externalViewSnapshot.equals(getExternalViews())
        && instanceConfigSnapshot.equals(getInstanceConfigMap()));
  }
}
