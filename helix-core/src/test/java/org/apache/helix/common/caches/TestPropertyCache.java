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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.helix.PropertyType.IDEALSTATES;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link PropertyCache}
 */
public class TestPropertyCache {
  private static final ControlContextProvider MOCK_CONTROL_CONTEXT_PROVIDER =
      new ControlContextProvider() {
        @Override
        public String getClusterName() {
          return "mockCluster";
        }

        @Override
        public String getClusterEventId() {
          return "id";
        }

        @Override
        public void setClusterEventId(String eventId) {

        }

        @Override
        public String getPipelineName() {
          return "pipeline";
        }
      };

  @Test(description = "Unit test for simple cache refresh")
  public void testSimpleCacheRefresh() {
    PropertyCache.PropertyCacheKeyFuncs propertyCacheKeyFuncs =
        mock(PropertyCache.PropertyCacheKeyFuncs.class);
    // Return a random property key, it does not impact test result.
    when(propertyCacheKeyFuncs.getRootKey(any(HelixDataAccessor.class)))
        .thenReturn(new PropertyKey(IDEALSTATES, IdealState.class, "Foobar"));

    PropertyCache<HelixProperty> propertyCache =
        new PropertyCache<>(MOCK_CONTROL_CONTEXT_PROVIDER, "mock property cache",
            propertyCacheKeyFuncs, false);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    Map<String, HelixProperty> propertyConfigMap = ImmutableMap.of("id", new HelixProperty("test"));
    when(accessor.getChildValuesMap(any(PropertyKey.class), anyBoolean()))
        .thenReturn(propertyConfigMap);

    propertyCache.refresh(accessor);

    Assert.assertEquals(propertyCache.getPropertyMap(), propertyConfigMap);
    Assert.assertEquals(propertyCache.getPropertyByName("id"), new HelixProperty("test"));
  }

  @Test(description = "Unit test for selective cache refresh")
  public void testSelectivePropertyRefreshInputs() {
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    Map<String, HelixProperty> currentCache = ImmutableMap.of("instance0",
        new HelixProperty("key0"), "instance1", new HelixProperty("key1"));

    PropertyCache.PropertyCacheKeyFuncs<HelixProperty> mockCacheKeyFuncs =
        new PropertyCache.PropertyCacheKeyFuncs<HelixProperty>() {
          @Override
          public PropertyKey getRootKey(HelixDataAccessor accessor) {
            return mock(PropertyKey.class);
          }

          @Override
          public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
            return new PropertyKey.Builder("fake").instance(objName);
          }

          @Override
          public String getObjName(HelixProperty obj) {
            return obj.getRecord().getId();
          }
        };
    when(accessor.getChildNames(any(PropertyKey.class)))
        .thenReturn(ImmutableList.of("instance1", "instance2"));
    @SuppressWarnings("unchecked")
    PropertyCache<HelixProperty> propertyCache = new PropertyCache<>(MOCK_CONTROL_CONTEXT_PROVIDER,
        "mock property cache", mock(PropertyCache.PropertyCacheKeyFuncs.class), false);

    PropertyCache.SelectivePropertyRefreshInputs<HelixProperty> selectivePropertyRefreshInputs =
        propertyCache.genSelectiveUpdateInput(accessor, currentCache, mockCacheKeyFuncs);

    Assert.assertEquals(selectivePropertyRefreshInputs.getReloadKeys().size(), 1);
    Assert.assertEquals(selectivePropertyRefreshInputs.getReloadKeys().get(0),
        new PropertyKey.Builder("fake").instance("instance2"));
  }

  @Test(description = "First set the property cache and update the object from caller")
  public void testDefensiveCopyOnDataUpdate() {
    @SuppressWarnings("unchecked")
    PropertyCache<HelixProperty> propertyCache = new PropertyCache<>(MOCK_CONTROL_CONTEXT_PROVIDER,
        "mock property cache", mock(PropertyCache.PropertyCacheKeyFuncs.class), false);
    HelixProperty helixProperty = new HelixProperty("id");
    Map<String, HelixProperty> propertyConfigMap = new HashMap<>();

    propertyCache.setPropertyMap(propertyConfigMap);
    // increment the property map from outside
    propertyConfigMap.put("id", helixProperty);

    Assert.assertTrue(propertyCache.getPropertyMap().isEmpty());
  }

  //TODO investigate if deep copy is needed for PropertyCache
  @Test(enabled = false, description = "First set the property cache and mutate the object from caller")
  public void testDefensiveCopyOnDataMutate() {
    // init
    @SuppressWarnings("unchecked")
    PropertyCache<InstanceConfig> propertyCache = new PropertyCache<>(MOCK_CONTROL_CONTEXT_PROVIDER,
        "mock property cache", mock(PropertyCache.PropertyCacheKeyFuncs.class), false);
    InstanceConfig instanceConfig = new InstanceConfig("id");
    Map<String, InstanceConfig> propertyConfigMap = ImmutableMap.of("id", instanceConfig);

    propertyCache.setPropertyMap(propertyConfigMap);
    // mutate the property from outside
    instanceConfig.setHostName("fakeHost");
    String hostName = propertyCache.getPropertyByName("id").getHostName();
    Assert.assertTrue(hostName.isEmpty());
  }
}
