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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.model.CustomizedView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cache to hold all CustomizedView of a specific type.
 */
public class CustomizedViewCache extends AbstractDataCache<CustomizedView> {
  private static final Logger LOG = LoggerFactory.getLogger(CustomizedViewCache.class.getName());

  private final PropertyCache<CustomizedView> _customizedViewCache;
  protected String _clusterName;
  private PropertyType _propertyType;
  private String _customizedStateType;

  public CustomizedViewCache(String clusterName, String customizedStateType) {
    this(clusterName, PropertyType.CUSTOMIZEDVIEW, customizedStateType);
  }

  protected CustomizedViewCache(String clusterName, PropertyType propertyType, String customizedStateType) {
      super(createDefaultControlContextProvider(clusterName));
      _clusterName = clusterName;
      _propertyType = propertyType;
      _customizedStateType = customizedStateType;
      _customizedViewCache = new PropertyCache<>(AbstractDataCache.createDefaultControlContextProvider(clusterName), "CustomizedView", new PropertyCache.PropertyCacheKeyFuncs<CustomizedView>() {
        @Override
        public PropertyKey getRootKey(HelixDataAccessor accessor) {
          return accessor.keyBuilder().customizedView(_customizedStateType);
        }

        @Override
        public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
          return accessor.keyBuilder().customizedView(_customizedStateType, objName);
        }

        @Override
        public String getObjName(CustomizedView obj) {
          return obj.getResourceName();
        }
      }, true);
    }


  /**
   * This refreshes the CustomizedView data by re-fetching the data from zookeeper in an efficient
   * way
   * @param accessor
   * @return
   */
  public void refresh(HelixDataAccessor accessor) {
    _customizedViewCache.refresh(accessor);
  }

  /**
   * Return CustomizedView map for all resources.
   * @return
   */
  public Map<String, CustomizedView> getCustomizedViewMap() {
    return Collections.unmodifiableMap(_customizedViewCache.getPropertyMap());
  }

  /**
   * Return customized view cache as a property cache.
   * @return
   */
  public PropertyCache<CustomizedView> getCustomizedViewCache() {
    return _customizedViewCache;
  }
}
