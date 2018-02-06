package org.apache.helix.spectator;

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

import org.apache.helix.HelixConstants;
import org.apache.helix.PropertyType;
import org.apache.helix.common.caches.BasicClusterDataCache;

/**
 * Cache the cluster data that are needed by RoutingTableProvider.
 */
public class RoutingDataCache extends BasicClusterDataCache {
  public RoutingDataCache(String clusterName, PropertyType sourceDataType) {
    super(clusterName, sourceDataType);
    requireFullRefresh();
  }

  /**
   * Notify the cache that some part of the cluster data has been changed.
   */
  public void notifyDataChange(HelixConstants.ChangeType changeType, String pathChanged) {
    _propertyDataChangedMap.put(changeType, Boolean.valueOf(true));
  }
}

