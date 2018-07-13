package org.apache.helix.common.caches;

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

import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataCache {
  private static Logger LOG = LoggerFactory.getLogger(AbstractDataCache.class.getName());
  private String _eventId = "NO_ID";

  public String getEventId() {
    return _eventId;
  }

  public void setEventId(String eventId) {
    _eventId = eventId;
  }

  /**
   * Selectively fetch Helix Properties from ZK by comparing the version of local cached one with the one on ZK.
   * If version on ZK is newer, fetch it from zk and update local cache.
   * @param accessor the HelixDataAccessor
   * @param reloadKeys keys needs to be reload
   * @param cachedKeys keys already exists in the cache
   * @param cachedPropertyMap cached map of propertykey -> property object
   * @param <T> the type of metadata
   * @return updated properties map
   */
  protected  <T extends HelixProperty> Map<PropertyKey, T> refreshProperties(
      HelixDataAccessor accessor, List<PropertyKey> reloadKeys, List<PropertyKey> cachedKeys,
      Map<PropertyKey, T> cachedPropertyMap) {
    // All new entries from zk not cached locally yet should be read from ZK.
    Map<PropertyKey, T> refreshedPropertyMap = Maps.newHashMap();
    List<HelixProperty.Stat> stats = accessor.getPropertyStats(cachedKeys);
    for (int i = 0; i < cachedKeys.size(); i++) {
      PropertyKey key = cachedKeys.get(i);
      HelixProperty.Stat stat = stats.get(i);
      if (stat != null) {
        T property = cachedPropertyMap.get(key);

        if (property != null && property.getBucketSize() == 0 && property.getStat().equals(stat)) {
          refreshedPropertyMap.put(key, property);
        } else {
          // need update from zk
          reloadKeys.add(key);
        }
      } else {
        LOG.warn("stat is null for key: " + key);
        reloadKeys.add(key);
      }
    }

    List<T> reloadedProperty = accessor.getProperty(reloadKeys, true);
    Iterator<PropertyKey> csKeyIter = reloadKeys.iterator();
    for (T property : reloadedProperty) {
      PropertyKey key = csKeyIter.next();
      if (property != null) {
        refreshedPropertyMap.put(key, property);
      } else {
        LOG.warn("znode is null for key: " + key);
      }
    }

    return refreshedPropertyMap;
  }

}
