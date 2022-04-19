package org.apache.helix.rest.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.junit.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestZoneAccessor extends AbstractTestClass  {

  private HelixDataAccessor _helixDataAccessor;
  private static final String TEST_CLUSTER = "TestCluster_zoneAccessor";
  private final String _instance1 = TEST_CLUSTER + "localhost_12918";
  private final String _instance2 = TEST_CLUSTER + "localhost_12922";
  private final String _instance3 = TEST_CLUSTER + "localhost_12923";
  private final String _instanceUrl1 =
      String.format("clusters/%s/instances/%s", TEST_CLUSTER, _instance1);
  private final String _instanceUrl2 =
      String.format("clusters/%s/instances/%s", TEST_CLUSTER, _instance2);
  private final String _instanceUrl3 =
      String.format("clusters/%s/instances/%s", TEST_CLUSTER, _instance3);

  @BeforeTest
  public void beforeTest() {
    _gSetupTool.addCluster(TEST_CLUSTER, true);
    _clusters.add(TEST_CLUSTER);
    Set<String> instances = createInstances(TEST_CLUSTER, 10);
    ClusterConfig clusterConfig = new ClusterConfig(TEST_CLUSTER);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
    clusterConfig.setTopologyAwareEnabled(true);
    _configAccessor.setClusterConfig(TEST_CLUSTER, clusterConfig);
    _clusterControllerManagers.add(startController(TEST_CLUSTER));

    _helixDataAccessor = new ZKHelixDataAccessor(TEST_CLUSTER, _baseAccessor);
    // setup up 10 instances across 5 zones
    for (int i = 0; i < 10; i++) {
      String instanceName = TEST_CLUSTER + "localhost_" + (12918 + i);
      InstanceConfig instanceConfig =
          _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().instanceConfig(instanceName));
      instanceConfig.setDomain("zone=zone_" + i / 2 + ",instance=" + instanceName);
      _helixDataAccessor.setProperty(_helixDataAccessor.keyBuilder().instanceConfig(instanceName), instanceConfig);
    }
    startInstances(TEST_CLUSTER, instances, 10);
  }

  @Test
  public void testDisabledZones() throws JsonProcessingException {
    String zonesUrl = String.format("clusters/%s/zones", TEST_CLUSTER);
    String disableUrl = String.format("clusters/%s/zones/disabledZones", TEST_CLUSTER);

    Set<String> zones = getSetFromRest(zonesUrl);
    Assert.assertEquals(zones.size(), 5);

    Set<String> disabledZones = getMapFromRest(disableUrl).keySet();
    Assert.assertTrue(disabledZones.isEmpty());

    post(zonesUrl + "/zone_2",
        ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    disabledZones = getMapFromRest(disableUrl).keySet();
    Assert.assertEquals(disabledZones.size(), 1);
    Assert.assertTrue(disabledZones.contains("zone_2"));

    post(zonesUrl + "/zone_1",
        ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(zonesUrl + "/zone_2",
        ImmutableMap.of("command", "enable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(zonesUrl + "/zone_3",
        ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(zonesUrl + "/zone_4",
        ImmutableMap.of("command", "enable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    disabledZones = getMapFromRest(disableUrl).keySet();
    Assert.assertEquals(disabledZones.size(), 2);
    Assert.assertTrue(disabledZones.contains("zone_1"));
    Assert.assertTrue(disabledZones.contains("zone_3"));

    post(zonesUrl + "/zone_1",
        ImmutableMap.of("command", "enable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(zonesUrl + "/zone_3",
        ImmutableMap.of("command", "enable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
  }

  @Test(dependsOnMethods = "testDisabledZones")
  public void testDisabledInstancesFromCache() {
    BaseControllerDataProvider cache = new BaseControllerDataProvider(TEST_CLUSTER, "test-pipeline");
    String zonesUrl = String.format("clusters/%s/zones", TEST_CLUSTER);

    post(zonesUrl + "/zone_2",
        ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(_instanceUrl1,
        ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(_instanceUrl2,
        ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(_instanceUrl3,
        ImmutableMap.of("command", "enable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    cache.refresh(_helixDataAccessor);
    Set<String> disabledInstancesFromCache = cache.getDisabledInstances();
    Assert.assertEquals(disabledInstancesFromCache.size(), 3);
    Assert.assertTrue(disabledInstancesFromCache.contains(_instance1));
    Assert.assertTrue(disabledInstancesFromCache.contains(_instance2));
    Assert.assertTrue(disabledInstancesFromCache.contains(_instance3));
  }

  private Map<String, String> getMapFromRest(String url) throws JsonProcessingException {
    String response = get(url, null, Response.Status.OK.getStatusCode(), true);
    return OBJECT_MAPPER.readValue(response, new TypeReference<HashMap<String, String>>() { });
  }

  private Set<String> getSetFromRest(String url) throws JsonProcessingException {
    String response = get(url, null, Response.Status.OK.getStatusCode(), true);
    return OBJECT_MAPPER.readValue(response, new TypeReference<HashSet<String>>() { });
  }
}
