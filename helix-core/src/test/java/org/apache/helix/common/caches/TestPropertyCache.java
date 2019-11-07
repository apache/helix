package org.apache.helix.common.caches;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

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
    @SuppressWarnings("unchecked")
    PropertyCache<HelixProperty> propertyCache = new PropertyCache<>(MOCK_CONTROL_CONTEXT_PROVIDER,
        "mock property cache", mock(PropertyCache.PropertyCacheKeyFuncs.class), false);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    Map<String, HelixProperty> propertyConfigMap = ImmutableMap.of("id", new HelixProperty("test"));
    when(accessor.getChildValuesMap(any(PropertyKey.class), any(Boolean.class)))
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
