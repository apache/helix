package org.apache.helix.cloud.virtualTopologyGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.cloud.topology.InstanceCountImbalanceAlgorithm;
import org.apache.helix.cloud.topology.InstanceWeightImbalanceAlgorithm;
import org.apache.helix.model.InstanceConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestImbalanceDetectionAlgorithm {
  private static final String CLUSTER_NAME = "TEST_NAME";
  private ConfigAccessor _configAccessor;
  private Map<String, Set<String>> _zoneMapping = new HashMap<>();

  @BeforeTest
  public void prepare() {
    _zoneMapping = new HashMap<>();
    _zoneMapping.put("zone1", Sets.newHashSet("9", "8", "7"));
    _zoneMapping.put("zone2", Sets.newHashSet("2", "3", "1"));
    _zoneMapping.put("zone3", Sets.newHashSet("b", "c", "d", "a", "e"));
    _zoneMapping.put("zone4", Sets.newHashSet("5", "4", "6"));

    _configAccessor = Mockito.mock(ConfigAccessor.class);
  }

  @Test
  public void testInstanceCountAlgorithm() {
    InstanceCountImbalanceAlgorithm algorithm = InstanceCountImbalanceAlgorithm.getInstance();
    Assert.assertFalse(algorithm.getImbalanceScore(_zoneMapping) > 2);
    Assert.assertTrue(algorithm.getImbalanceScore(_zoneMapping) > 1);
  }

  @Test
  public void testInstanceWeightAlgorithm() {
    InstanceConfig defaultWeightConfig = new InstanceConfig.Builder().setWeight(1000).build("");
    InstanceConfig largeWeightConfig = new InstanceConfig.Builder().setWeight(2000).build("");
    InstanceWeightImbalanceAlgorithm algorithm =
        InstanceWeightImbalanceAlgorithm.getInstance(_configAccessor, CLUSTER_NAME);
    // Zone 1 -> 3000
    // Zone 2 -> 4000
    // Zone 3 -> 5000
    // Zone 4 -> 3000
    Mockito.when(_configAccessor.getInstanceConfig(Mockito.eq(CLUSTER_NAME), Mockito.anyString()))
        .thenReturn(defaultWeightConfig);

    // 2) Now override for specific instance names:
    Mockito.when(_configAccessor.getInstanceConfig(Mockito.eq(CLUSTER_NAME), Mockito.eq("1")))
        .thenReturn(largeWeightConfig);

    Assert.assertTrue(algorithm.getImbalanceScore(_zoneMapping) > 1000);
    Assert.assertFalse(algorithm.getImbalanceScore(_zoneMapping) > 2000);

    // Change the instance weight
    // Zone 1 -> 4000
    // Zone 2 -> 4000
    // Zone 3 -> 5000
    // Zone 4 -> 4000
    Mockito.when(_configAccessor.getInstanceConfig(Mockito.eq(CLUSTER_NAME), Mockito.eq("4")))
        .thenReturn(largeWeightConfig);
    Mockito.when(_configAccessor.getInstanceConfig(Mockito.eq(CLUSTER_NAME), Mockito.eq("7")))
        .thenReturn(largeWeightConfig);

    Assert.assertFalse(algorithm.getImbalanceScore(_zoneMapping) > 1000);
    Assert.assertFalse(algorithm.getImbalanceScore(_zoneMapping) > 2000);
    Assert.assertTrue(algorithm.getImbalanceScore(_zoneMapping) > 500);
  }
}
