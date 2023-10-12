package org.apache.helix.rest.common.dataprovider;

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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.caches.PropertyCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for caching simple HelixProperty objects.
 * @param <T>
 */
public class RestPropertyCache<T extends HelixProperty> {
  private static final Logger LOG = LoggerFactory.getLogger(RestPropertyCache.class);

  private ConcurrentHashMap<String, T> _objCache;
  private final String _propertyDescription;

  private final RestPropertyCache.PropertyCacheKeyFuncs<T> _keyFuncs;

  public interface PropertyCacheKeyFuncs<O extends HelixProperty> {
    /**
     * Get PropertyKey for the root of this type of object, used for LIST all objects
     * @return property key to object root
     */
    PropertyKey getRootKey(HelixDataAccessor accessor);

    /**
     * Get PropertyKey for a single object of this type, used for GET single instance of the type
     * @param objName object name
     * @return property key to the object instance
     */
    PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName);

    /**
     * Get the string to identify the object when we actually use them. It's not necessarily the
     * "id" field of HelixProperty, but could have more semantic meanings of that object type
     * @param obj object instance
     * @return object identifier
     */
    String getObjName(O obj);
  }

  public RestPropertyCache(String propertyDescription, RestPropertyCache.PropertyCacheKeyFuncs<T> keyFuncs) {
    _keyFuncs = keyFuncs;
    _propertyDescription = propertyDescription;
  }

  public void init(final HelixDataAccessor accessor) {
    _objCache = new ConcurrentHashMap<>(accessor.getChildValuesMap(_keyFuncs.getRootKey(accessor), true));
    LOG.info("Init RestPropertyCache for {}. ", _propertyDescription);
  }

  public Map<String, T> getPropertyMap() {
    return Collections.unmodifiableMap(_objCache);
  }

}
