package org.apache.helix.rest.server;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.sun.research.ws.wadl.HTTPMethods;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.cloud.azure.AzureConstants;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.rest.server.resources.AbstractResource.Command;
import org.apache.helix.rest.server.resources.helix.ClusterAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterAccessor extends AbstractTestClass {

  @BeforeClass
  public void beforeClass() {
    for (String cluster : _clusters) {
      ClusterConfig clusterConfig = createClusterConfig(cluster);
      _configAccessor.setClusterConfig(cluster, clusterConfig);
    }
  }

  @Test
  public void testGetClusters() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    _auditLogger.clearupLogs();
    String body = get("clusters", null, Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String clustersStr = node.get(ClusterAccessor.ClusterProperties.clusters.name()).toString();
    Assert.assertNotNull(clustersStr);

    Set<String> clusters = OBJECT_MAPPER.readValue(clustersStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(clusters, _clusters,
        "clusters from response: " + clusters + " vs clusters actually: " + _clusters);

    Assert.assertEquals(_auditLogger.getAuditLogs().size(), 1);
    AuditLog auditLog = _auditLogger.getAuditLogs().get(0);
    validateAuditLog(auditLog, HTTPMethods.GET.name(), "clusters",
        Response.Status.OK.getStatusCode(), body);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetClusters")
  public void testGetClusterTopology() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = "TestCluster_1";
    String instance = cluster + "localhost_12920";
    // set the fake zone id in instance configuration
    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    InstanceConfig instanceConfig =
        helixDataAccessor.getProperty(helixDataAccessor.keyBuilder().instanceConfig(instance));
    instanceConfig.setDomain("helixZoneId=123");
    helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().instanceConfig(instance),
        instanceConfig);

    String response = new JerseyUriRequestBuilder("clusters/{}/topology").format(cluster).get(this);

    Assert.assertEquals(response,
        "{\"id\":\"TestCluster_1\",\"zones\":[{\"id\":\"123\",\"instances\":[{\"id\":\"TestCluster_1localhost_12920\"}]}],"
            + "\"allInstances\":[\"TestCluster_1localhost_12918\",\"TestCluster_1localhost_12919\",\"TestCluster_1localhost_12924\","
            + "\"TestCluster_1localhost_12925\",\"TestCluster_1localhost_12926\",\"TestCluster_1localhost_12927\",\"TestCluster_1localhost_12920\","
            + "\"TestCluster_1localhost_12921\",\"TestCluster_1localhost_12922\",\"TestCluster_1localhost_12923\"]}");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetClusterTopology")
  public void testGetClusterTopologyAndFaultZoneMap() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String topologyMapUrlBase = "clusters/TestCluster_1/topologymap/";
    String faultZoneUrlBase = "clusters/TestCluster_1/faultzonemap/";

    // test invalid case where instance config and cluster topology have not been set.
    get(topologyMapUrlBase, null, Response.Status.BAD_REQUEST.getStatusCode(), true);
    get(faultZoneUrlBase, null, Response.Status.BAD_REQUEST.getStatusCode(), true);

    String cluster = "TestCluster_1";
    for (int i = 0; i < 5; i++) {
      String instance = cluster + "localhost_129" + String.valueOf(18 + i);
      HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
      InstanceConfig instanceConfig =
          helixDataAccessor.getProperty(helixDataAccessor.keyBuilder().instanceConfig(instance));
      instanceConfig.setDomain("helixZoneId=zone0,instance=" + instance);
      helixDataAccessor
          .setProperty(helixDataAccessor.keyBuilder().instanceConfig(instance), instanceConfig);
    }

    for (int i = 0; i < 5; i++) {
      String instance = cluster + "localhost_129" + String.valueOf(23 + i);
      HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
      InstanceConfig instanceConfig =
          helixDataAccessor.getProperty(helixDataAccessor.keyBuilder().instanceConfig(instance));
      instanceConfig.setDomain("helixZoneId=zone1,instance=" + instance);
      helixDataAccessor
          .setProperty(helixDataAccessor.keyBuilder().instanceConfig(instance), instanceConfig);
    }

    // test invalid case where instance config is set, but cluster topology has not been set.
    get(topologyMapUrlBase, null, Response.Status.BAD_REQUEST.getStatusCode(), true);
    get(faultZoneUrlBase, null, Response.Status.BAD_REQUEST.getStatusCode(), true);

    ClusterConfig configDelta = new ClusterConfig(cluster);
    configDelta.getRecord().setSimpleField("TOPOLOGY", "/helixZoneId/instance");
    updateClusterConfigFromRest(cluster, configDelta, Command.update);

    //get valid cluster topology map
    String topologyMapDef = get(topologyMapUrlBase, null, Response.Status.OK.getStatusCode(), true);
    Map<String, Object> topologyMap =
        OBJECT_MAPPER.readValue(topologyMapDef, new TypeReference<HashMap<String, Object>>() {
        });
    Assert.assertEquals(topologyMap.size(), 2);
    Assert.assertTrue(topologyMap.get("/helixZoneId:zone0") instanceof List);
    List<String> instances = (List<String>) topologyMap.get("/helixZoneId:zone0");
    Assert.assertEquals(instances.size(), 5);
    Assert.assertTrue(instances.containsAll(new HashSet<>(Arrays
        .asList("/instance:TestCluster_1localhost_12918",
            "/instance:TestCluster_1localhost_12919",
            "/instance:TestCluster_1localhost_12920",
            "/instance:TestCluster_1localhost_12921",
            "/instance:TestCluster_1localhost_12922"))));

    Assert.assertTrue(topologyMap.get("/helixZoneId:zone1") instanceof List);
    instances = (List<String>) topologyMap.get("/helixZoneId:zone1");
    Assert.assertEquals(instances.size(), 5);
    Assert.assertTrue(instances.containsAll(new HashSet<>(Arrays
        .asList("/instance:TestCluster_1localhost_12923",
            "/instance:TestCluster_1localhost_12924",
            "/instance:TestCluster_1localhost_12925",
            "/instance:TestCluster_1localhost_12926",
            "/instance:TestCluster_1localhost_12927"))));

    configDelta = new ClusterConfig(cluster);
    configDelta.getRecord().setSimpleField("FAULT_ZONE_TYPE", "helixZoneId");
    updateClusterConfigFromRest(cluster, configDelta, Command.update);

    //get valid cluster fault zone map
    String faultZoneMapDef = get(faultZoneUrlBase, null, Response.Status.OK.getStatusCode(), true);
    Map<String, Object> faultZoneMap =
        OBJECT_MAPPER.readValue(faultZoneMapDef, new TypeReference<HashMap<String, Object>>() {
        });
    Assert.assertEquals(faultZoneMap.size(), 2);
    Assert.assertTrue(faultZoneMap.get("/helixZoneId:zone0") instanceof List);
    instances = (List<String>) faultZoneMap.get("/helixZoneId:zone0");
    Assert.assertEquals(instances.size(), 5);
    Assert.assertTrue(instances.containsAll(new HashSet<>(Arrays
        .asList("/instance:TestCluster_1localhost_12918",
            "/instance:TestCluster_1localhost_12919",
            "/instance:TestCluster_1localhost_12920",
            "/instance:TestCluster_1localhost_12921",
            "/instance:TestCluster_1localhost_12922"))));

    Assert.assertTrue(faultZoneMap.get("/helixZoneId:zone1") instanceof List);
    instances = (List<String>) faultZoneMap.get("/helixZoneId:zone1");
    Assert.assertEquals(instances.size(), 5);
    Assert.assertTrue(instances.containsAll(new HashSet<>(Arrays
        .asList("/instance:TestCluster_1localhost_12923",
            "/instance:TestCluster_1localhost_12924",
            "/instance:TestCluster_1localhost_12925",
            "/instance:TestCluster_1localhost_12926",
            "/instance:TestCluster_1localhost_12927"))));
  }

  @Test(dependsOnMethods = "testGetClusterTopologyAndFaultZoneMap")
  public void testAddConfigFields() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    ClusterConfig oldConfig = getClusterConfigFromRest(cluster);

    ClusterConfig configDelta = new ClusterConfig(cluster);
    configDelta.getRecord().setSimpleField("newField", "newValue");
    configDelta.getRecord().setListField("newList", Arrays.asList("newValue1", "newValue2"));
    configDelta.getRecord().setMapField("newMap", new HashMap<String, String>() {
      {
        put("newkey1", "newvalue1");
        put("newkey2", "newvalue2");
      }
    });

    updateClusterConfigFromRest(cluster, configDelta, Command.update);

    ClusterConfig newConfig = getClusterConfigFromRest(cluster);
    oldConfig.getRecord().update(configDelta.getRecord());
    Assert.assertEquals(newConfig, oldConfig,
        "cluster config from response: " + newConfig + " vs cluster config actually: " + oldConfig);
    System.out.println("ZNode after testAddConfigFields oldConfig: " + oldConfig.toString());
    System.out.println("ZNode after testAddConfigFields newConfig: " + newConfig.toString());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddConfigFields")
  public void testUpdateConfigFields() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    ClusterConfig config = getClusterConfigFromRest(cluster);

    System.out.println("ZNode after testUpdateConfigFields config: " + config.toString());
    ZNRecord record = config.getRecord();

    String key = record.getSimpleFields().keySet().iterator().next();
    String value = record.getSimpleField(key);
    record.getSimpleFields().clear();
    record.setSimpleField(key, value + "--updated");

    key = record.getListFields().keySet().iterator().next();
    List<String> list = record.getListField(key);
    list.remove(0);
    list.add("newValue--updated");
    record.getListFields().clear();
    record.setListField(key, list);

    key = record.getMapFields().keySet().iterator().next();
    Map<String, String> map = record.getMapField(key);
    Iterator it = map.entrySet().iterator();
    it.next();
    it.remove();
    map.put("newKey--updated", "newValue--updated");
    record.getMapFields().clear();
    record.setMapField(key, map);

    ClusterConfig prevConfig = getClusterConfigFromRest(cluster);
    updateClusterConfigFromRest(cluster, config, Command.update);
    ClusterConfig newConfig = getClusterConfigFromRest(cluster);

    prevConfig.getRecord().update(config.getRecord());
    Assert.assertEquals(newConfig, prevConfig, "cluster config from response: " + newConfig
        + " vs cluster config actually: " + prevConfig);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testUpdateConfigFields")
  public void testDeleteConfigFields() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    ClusterConfig config = getClusterConfigFromRest(cluster);

    ZNRecord record = config.getRecord();

    String simpleKey = record.getSimpleFields().keySet().iterator().next();
    String value = record.getSimpleField(simpleKey);
    record.getSimpleFields().clear();
    record.setSimpleField(simpleKey, value);

    String listKey = record.getListFields().keySet().iterator().next();
    List<String> list = record.getListField(listKey);
    record.getListFields().clear();
    record.setListField(listKey, list);

    String mapKey = record.getMapFields().keySet().iterator().next();
    Map<String, String> map = record.getMapField(mapKey);
    record.getMapFields().clear();
    record.setMapField(mapKey, map);

    ClusterConfig prevConfig = getClusterConfigFromRest(cluster);
    updateClusterConfigFromRest(cluster, config, Command.delete);
    ClusterConfig newConfig = getClusterConfigFromRest(cluster);

    Assert.assertFalse(newConfig.getRecord().getSimpleFields().containsKey(simpleKey),
        "Failed to delete key " + simpleKey + " from cluster config");
    Assert.assertFalse(newConfig.getRecord().getListFields().containsKey(listKey),
        "Failed to delete key " + listKey + " from cluster config");
    Assert.assertFalse(newConfig.getRecord().getSimpleFields().containsKey(mapKey),
        "Failed to delete key " + mapKey + " from cluster config");

    prevConfig.getRecord().subtract(config.getRecord());
    Assert.assertEquals(newConfig, prevConfig, "cluster config from response: " + newConfig
        + " vs cluster config actually: " + prevConfig);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testDeleteConfigFields")
  public void testCreateDeleteCluster() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // create an existing cluster should fail.
    _auditLogger.clearupLogs();
    String cluster = _clusters.iterator().next();
    put("clusters/" + cluster, null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // create a new cluster
    cluster = "NewCluster";
    put("clusters/" + cluster, null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // verify the cluster has been created.
    Assert.assertTrue(ZKUtil.isClusterSetup(cluster, _gZkClient));

    // delete the cluster
    delete("clusters/" + cluster, Response.Status.OK.getStatusCode());

    // verify the cluster has been deleted.
    Assert.assertFalse(_baseAccessor.exists("/" + cluster, 0));
    Assert.assertEquals(_auditLogger.getAuditLogs().size(), 3);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testCreateDeleteCluster")
  public void testEnableDisableCluster() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // disable a cluster.
    String cluster = _clusters.iterator().next();
    _auditLogger.clearupLogs();
    post("clusters/" + cluster, ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(cluster);
    // verify the cluster is paused.
    Assert.assertTrue(_baseAccessor.exists(keyBuilder.pause().getPath(), 0));

    // enable a cluster.
    post("clusters/" + cluster, ImmutableMap.of("command", "enable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // verify the cluster is paused.
    Assert.assertFalse(_baseAccessor.exists(keyBuilder.pause().getPath(), 0));
    Assert.assertEquals(_auditLogger.getAuditLogs().size(), 2);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testEnableDisableCluster")
  public void testGetClusterConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Response response = target("clusters/fakeCluster/configs").request().get();
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    String cluster = _clusters.iterator().next();
    getClusterConfigFromRest(cluster);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetClusterConfig")
  public void testEnableDisableMaintenanceMode() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    String reason = "Test reason";
    // enable maintenance mode
    post("clusters/" + cluster, ImmutableMap.of("command", "enableMaintenanceMode"),
        Entity.entity(reason, MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode());

    // verify is in maintenance mode
    String body =
        get("clusters/" + cluster + "/maintenance", null, Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    boolean maintenance =
        node.get(ClusterAccessor.ClusterProperties.maintenance.name()).booleanValue();
    Assert.assertTrue(maintenance);

    // Check that we could retrieve maintenance signal correctly
    String signal = get("clusters/" + cluster + "/controller/maintenanceSignal", null,
        Response.Status.OK.getStatusCode(), true);
    Map<String, Object> maintenanceSignalMap =
        OBJECT_MAPPER.readValue(signal, new TypeReference<HashMap<String, Object>>() {
        });
    Assert.assertEquals(maintenanceSignalMap.get("TRIGGERED_BY"), "USER");
    Assert.assertEquals(maintenanceSignalMap.get("REASON"), reason);
    Assert.assertNotNull(maintenanceSignalMap.get("TIMESTAMP"));
    Assert.assertEquals(maintenanceSignalMap.get("clusterName"), cluster);

    // disable maintenance mode
    post("clusters/" + cluster, ImmutableMap.of("command", "disableMaintenanceMode"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode());

    // verify no longer in maintenance mode
    body = get("clusters/" + cluster + "/maintenance", null, Response.Status.OK.getStatusCode(), true);
    node = OBJECT_MAPPER.readTree(body);
    Assert.assertFalse(
        node.get(ClusterAccessor.ClusterProperties.maintenance.name()).booleanValue());

    get("clusters/" + cluster + "/controller/maintenanceSignal", null,
        Response.Status.NOT_FOUND.getStatusCode(), false);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testEnableDisableMaintenanceMode")
  public void testGetControllerLeadershipHistory() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();

    // Get the leader controller name for the cluster
    String leader =
        get("clusters/" + cluster + "/controller", null, Response.Status.OK.getStatusCode(), true);
    Map<String, String> leaderMap =
        OBJECT_MAPPER.readValue(leader, new TypeReference<HashMap<String, String>>() {
        });
    Assert.assertNotNull(leaderMap, "Controller leader cannot be null!");
    leader = leaderMap.get("controller");
    Assert.assertNotNull(leader, "Leader name cannot be null!");

    // Get the controller leadership history JSON's last entry
    String leadershipHistory = get("clusters/" + cluster + "/controller/history", null,
        Response.Status.OK.getStatusCode(), true);
    Map<String, Object> leadershipHistoryMap =
        OBJECT_MAPPER.readValue(leadershipHistory, new TypeReference<HashMap<String, Object>>() {
        });
    Assert.assertNotNull(leadershipHistoryMap, "Leadership history cannot be null!");
    Object leadershipHistoryList =
        leadershipHistoryMap.get(AbstractResource.Properties.history.name());
    Assert.assertNotNull(leadershipHistoryList);
    List<?> list = (List<?>) leadershipHistoryList;
    Assert.assertTrue(list.size() > 0);
    String lastLeaderEntry = (String) list.get(list.size() - 1);

    // Check that the last entry contains the current leader name
    Assert.assertTrue(lastLeaderEntry.contains(leader));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetControllerLeadershipHistory")
  public void testGetMaintenanceHistory() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    String reason = TestHelper.getTestMethodName();

    // Enable maintenance mode
    post("clusters/" + cluster, ImmutableMap.of("command", "enableMaintenanceMode"),
        Entity.entity(reason, MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode());

    // Get the maintenance history JSON's last entry
    String maintenanceHistory = get("clusters/" + cluster + "/controller/maintenanceHistory", null,
        Response.Status.OK.getStatusCode(), true);
    Map<String, Object> maintenanceHistoryMap =
        OBJECT_MAPPER.readValue(maintenanceHistory, new TypeReference<HashMap<String, Object>>() {
        });
    Object maintenanceHistoryList =
        maintenanceHistoryMap.get(ClusterAccessor.ClusterProperties.maintenanceHistory.name());
    Assert.assertNotNull(maintenanceHistoryList);
    List<?> list = (List<?>) maintenanceHistoryList;
    Assert.assertTrue(list.size() > 0);
    String lastMaintenanceEntry = (String) list.get(list.size() - 1);

    // Check that the last entry contains the reason string
    Assert.assertTrue(lastMaintenanceEntry.contains(reason));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetMaintenanceHistory")
  public void testEnableDisableMaintenanceModeWithCustomFields() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    HelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);

    String content = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
    post("clusters/" + cluster, ImmutableMap.of("command", "enableMaintenanceMode"),
        Entity.entity(content, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    MaintenanceSignal signal = accessor.getProperty(accessor.keyBuilder().maintenance());
    Assert.assertNotNull(signal);
    Assert.assertNull(signal.getReason());
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Map<String, String> simpleFields = signal.getRecord().getSimpleFields();
    Assert.assertEquals(simpleFields.get("key1"), "value1");
    Assert.assertEquals(simpleFields.get("key2"), "value2");

    post("clusters/" + cluster, ImmutableMap.of("command", "disableMaintenanceMode"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode());
    Assert.assertFalse(
        accessor.getBaseDataAccessor().exists(accessor.keyBuilder().maintenance().getPath(), 0));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testEnableDisableMaintenanceModeWithCustomFields")
  public void testPurgeOfflineParticipants() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    HelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);

    String instance1 = cluster + "localhost_12923";
    String instance2 = cluster + "localhost_12924";
    String instance3 = cluster + "localhost_12926";
    post("clusters/" + cluster,
        ImmutableMap.of("command", "purgeOfflineParticipants", "duration", "100000000"), null,
        Response.Status.OK.getStatusCode());

    //Although the three instances are not in live instances, the timeout is not met, and
    // instances will not be dropped by purging action
    Assert.assertTrue(accessor.getBaseDataAccessor()
        .exists(accessor.keyBuilder().instanceConfig(instance1).getPath(), 0));
    Assert.assertTrue(accessor.getBaseDataAccessor()
        .exists(accessor.keyBuilder().instanceConfig(instance2).getPath(), 0));
    Assert.assertTrue(accessor.getBaseDataAccessor()
        .exists(accessor.keyBuilder().instanceConfig(instance3).getPath(), 0));

    ClusterConfig configDelta = new ClusterConfig(cluster);
    configDelta.getRecord()
        .setSimpleField(ClusterConfig.ClusterConfigProperty.OFFLINE_DURATION_FOR_PURGE_MS.name(),
            "100");
    updateClusterConfigFromRest(cluster, configDelta, Command.update);

    //Purge again without customized timeout, and the action will use default timeout value.
    post("clusters/" + cluster, ImmutableMap.of("command", "purgeOfflineParticipants"), null,
        Response.Status.OK.getStatusCode());
    Assert.assertFalse(accessor.getBaseDataAccessor()
        .exists(accessor.keyBuilder().instanceConfig(instance1).getPath(), 0));
    Assert.assertFalse(accessor.getBaseDataAccessor()
        .exists(accessor.keyBuilder().instanceConfig(instance2).getPath(), 0));
    Assert.assertFalse(accessor.getBaseDataAccessor()
        .exists(accessor.keyBuilder().instanceConfig(instance3).getPath(), 0));

    // reset cluster status to previous one
    _gSetupTool.addInstanceToCluster(cluster, instance1);
    _gSetupTool.addInstanceToCluster(cluster, instance2);
    _gSetupTool.addInstanceToCluster(cluster, instance3);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testEnableDisableMaintenanceModeWithCustomFields")
  public void testGetStateModelDef() throws IOException {

    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = "TestCluster_1";
    String urlBase = "clusters/TestCluster_1/statemodeldefs/";
    String stateModelDefs =
        get(urlBase, null, Response.Status.OK.getStatusCode(), true);
    Map<String, Object> defMap = OBJECT_MAPPER.readValue(stateModelDefs, new TypeReference<HashMap<String, Object>>() {
    });

    Assert.assertTrue(defMap.size() == 2);
    Assert.assertTrue(defMap.get("stateModelDefinitions") instanceof List);
    List<String> stateModelNames = (List<String>) defMap.get("stateModelDefinitions");
    Assert.assertEquals(stateModelNames.size(), 7);

    String oneModel = stateModelNames.get(1);
    String twoModel = stateModelNames.get(2);

    String oneModelUri = urlBase + oneModel;
    String oneResult = get(oneModelUri, null, Response.Status.OK.getStatusCode(), true);
    ZNRecord oneRecord = OBJECT_MAPPER.readValue(oneResult, ZNRecord.class);

    String twoResult =
        get("clusters/" + cluster + "/statemodeldefs/" + twoModel, null, Response.Status.OK.getStatusCode(), true);
    ZNRecord twoRecord = OBJECT_MAPPER.readValue(twoResult, ZNRecord.class);

    // delete one, expect success
    String deleteOneUri = urlBase + oneRecord.getId();
    Response deleteOneRsp = target(deleteOneUri).request().delete();
    Assert.assertEquals(deleteOneRsp.getStatus(), Response.Status.OK.getStatusCode());

    Response queryRsp = target(oneModelUri).request().get();
    Assert.assertTrue(queryRsp.getStatus() == Response.Status.BAD_REQUEST.getStatusCode());

    // delete one again, expect success
    Response deleteOneRsp2 = target(deleteOneUri).request().delete();
    Assert.assertTrue(deleteOneRsp2.getStatus() == Response.Status.OK.getStatusCode());

    // create the delete one, expect success
    Response createOneRsp = target(oneModelUri).request()
        .put(Entity.entity(OBJECT_MAPPER.writeValueAsString(oneRecord), MediaType.APPLICATION_JSON_TYPE));
    Assert.assertTrue(createOneRsp.getStatus() == Response.Status.OK.getStatusCode());

    // create the delete one again, expect failure
    Response createOneRsp2 = target(oneModelUri).request()
        .put(Entity.entity(OBJECT_MAPPER.writeValueAsString(oneRecord), MediaType.APPLICATION_JSON_TYPE));
    Assert.assertTrue(createOneRsp2.getStatus() == Response.Status.BAD_REQUEST.getStatusCode());

    // set the delete one with a modification
    ZNRecord newRecord = new ZNRecord(twoRecord, oneRecord.getId());
    Response setOneRsp = target(oneModelUri).request()
        .post(Entity.entity(OBJECT_MAPPER.writeValueAsString(newRecord), MediaType.APPLICATION_JSON_TYPE));
    Assert.assertTrue(setOneRsp.getStatus() == Response.Status.OK.getStatusCode());

    String oneResult2 = get(oneModelUri, null, Response.Status.OK.getStatusCode(), true);
    ZNRecord oneRecord2 = OBJECT_MAPPER.readValue(oneResult2, ZNRecord.class);
    Assert.assertEquals(oneRecord2, newRecord);

    // set the delete one with original; namely restore the original condition
    Response setOneRsp2 = target(oneModelUri).request()
        .post(Entity.entity(OBJECT_MAPPER.writeValueAsString(oneRecord), MediaType.APPLICATION_JSON_TYPE));
    Assert.assertTrue(setOneRsp2.getStatus() == Response.Status.OK.getStatusCode());

    String oneResult3 = get(oneModelUri, null, Response.Status.OK.getStatusCode(), true);
    ZNRecord oneRecord3 = OBJECT_MAPPER.readValue(oneResult3, ZNRecord.class);
    Assert.assertEquals(oneRecord3, oneRecord);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetStateModelDef")
  public void testActivateSuperCluster() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    final String ACTIVATE_SUPER_CLUSTER = "RestSuperClusterActivationTest_SuperCluster";
    final String ACTIVATE_NORM_CLUSTER = "RestSuperClusterActivationTest_NormalCluster";

    // create testCluster
    _gSetupTool.addCluster(ACTIVATE_NORM_CLUSTER, true);
    ClusterConfig clusterConfig = new ClusterConfig(ACTIVATE_NORM_CLUSTER);
    clusterConfig.setFaultZoneType("helixZoneId");
    _configAccessor.setClusterConfig(ACTIVATE_NORM_CLUSTER, clusterConfig);
    Set<String> resources = createResourceConfigs(ACTIVATE_NORM_CLUSTER, 8);

    // create superCluster
    _gSetupTool.addCluster(ACTIVATE_SUPER_CLUSTER,true);
    ClusterConfig superClusterConfig = new ClusterConfig(ACTIVATE_SUPER_CLUSTER);
    _configAccessor.setClusterConfig(ACTIVATE_SUPER_CLUSTER, superClusterConfig);
    Set<String> instances = createInstances(ACTIVATE_SUPER_CLUSTER, 4);
    List<ClusterDistributedController> clusterDistributedControllers = new ArrayList<>();
    for (String instance : instances) {
      ClusterDistributedController controllerParticipant =
          new ClusterDistributedController(ZK_ADDR, ACTIVATE_SUPER_CLUSTER, instance);
      clusterDistributedControllers.add(controllerParticipant);
      controllerParticipant.syncStart();
    }

    post("clusters/" + ACTIVATE_NORM_CLUSTER,
        ImmutableMap.of("command", "activate", "superCluster", ACTIVATE_SUPER_CLUSTER),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE), Response.Status.OK .getStatusCode());

    HelixDataAccessor accessor = new ZKHelixDataAccessor(ACTIVATE_SUPER_CLUSTER, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    final HelixDataAccessor normalAccessor = new ZKHelixDataAccessor(ACTIVATE_NORM_CLUSTER, _baseAccessor);
    final PropertyKey.Builder normKeyBuilder = normalAccessor.keyBuilder();

    boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        LiveInstance leader = normalAccessor.getProperty(normKeyBuilder.controllerLeader());
        return leader != null;
      }
    }, 12000);
    Assert.assertTrue(result);

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(ACTIVATE_SUPER_CLUSTER).setZkAddr(ZK_ADDR)
            .setZkClient(_gZkClient).build();
    Assert.assertTrue(verifier.verifyByPolling());

    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(ACTIVATE_NORM_CLUSTER));
    Assert.assertEquals(idealState.getRebalanceMode(), IdealState.RebalanceMode.FULL_AUTO);
    Assert.assertEquals(idealState.getRebalancerClassName(), DelayedAutoRebalancer.class.getName());
    Assert.assertEquals(idealState.getRebalanceStrategy(), CrushEdRebalanceStrategy.class.getName());
    // Note, set expected replicas value to 3, as the same value of DEFAULT_SUPERCLUSTER_REPLICA in ClusterAccessor.
    Assert.assertEquals(idealState.getReplicas(), "3");


    ExternalView externalView = accessor.getProperty(keyBuilder.externalView(ACTIVATE_NORM_CLUSTER));
    Map<String, String> extViewMapping = externalView.getRecord().getMapField(ACTIVATE_NORM_CLUSTER);
    String superClusterleader = null;
    for (Map.Entry<String, String> entry: extViewMapping.entrySet()) {
      if (entry.getValue().equals("LEADER")) {
        superClusterleader = entry.getKey();
      }
    }
    LiveInstance leader = normalAccessor.getProperty(normKeyBuilder.controllerLeader());
    Assert.assertEquals(leader.getId(), superClusterleader);

    // clean up by tearing down controllers and delete clusters
    for (ClusterDistributedController dc: clusterDistributedControllers) {
      if (dc != null && dc.isConnected()) {
        dc.syncStop();
      }
    }
    _gSetupTool.deleteCluster(ACTIVATE_NORM_CLUSTER);
    _gSetupTool.deleteCluster(ACTIVATE_SUPER_CLUSTER);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testActivateSuperCluster")
  public void testEnableWagedRebalanceForAllResources() {
    String cluster = "TestCluster_2";
    post("clusters/" + cluster, ImmutableMap.of("command", "enableWagedRebalanceForAllResources"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode());
    for (String resource : _gSetupTool.getClusterManagementTool().getResourcesInCluster(cluster)) {
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
      Assert.assertEquals(idealState.getRebalancerClassName(), WagedRebalancer.class.getName());
    }
  }

  @Test
  public void testCreateRESTConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    RESTConfig restConfigRest = new RESTConfig(cluster);
    restConfigRest.set(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL, "http://*:00");
    put("clusters/" + cluster + "/restconfig", null, Entity
        .entity(OBJECT_MAPPER.writeValueAsString(restConfigRest.getRecord()),
            MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode());
    RESTConfig restConfigZK = _configAccessor.getRESTConfig(cluster);
    Assert.assertEquals(restConfigZK, restConfigRest,
        "rest config from response: " + restConfigRest + " vs rest config actually: "
            + restConfigZK);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testCreateRESTConfig")
  public void testUpdateRESTConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    RESTConfig restConfigRest = new RESTConfig(cluster);
    // Update an entry
    restConfigRest.set(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL, "http://*:01");
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(restConfigRest.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + cluster + "/restconfig", ImmutableMap.of("command", Command.update.name()),
        entity, Response.Status.OK.getStatusCode());
    RESTConfig restConfigZK = _configAccessor.getRESTConfig(cluster);
    Assert.assertEquals(restConfigZK, restConfigRest,
        "rest config from response: " + restConfigRest + " vs rest config actually: "
            + restConfigZK);

    // Delete an entry
    restConfigRest.set(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL, null);
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(restConfigRest.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + cluster + "/restconfig", ImmutableMap.of("command", Command.delete.name()),
        entity, Response.Status.OK.getStatusCode());
    restConfigZK = _configAccessor.getRESTConfig(cluster);
    Assert.assertEquals(restConfigZK, new RESTConfig(cluster),
        "rest config from response: " + new RESTConfig(cluster) + " vs rest config actually: "
            + restConfigZK);

    // Update a cluster rest config that the cluster does not exist
    String wrongClusterId = "wrong_cluster_id";
    restConfigRest = new RESTConfig(wrongClusterId);
    restConfigRest.set(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL, "http://*:01");
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(restConfigRest.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + wrongClusterId + "/restconfig",
        ImmutableMap.of("command", Command.update.name()), entity,
        Response.Status.NOT_FOUND.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testUpdateRESTConfig")
  public void testDeleteRESTConfig() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    delete("clusters/" + cluster + "/restconfig", Response.Status.OK.getStatusCode());
    get("clusters/" + cluster + "/restconfig", null, Response.Status.NOT_FOUND.getStatusCode(), true);
    delete("clusters/" + cluster + "/restconfig", Response.Status.OK.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testActivateSuperCluster")
  public void testAddClusterWithCloudConfig() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    ZNRecord record = new ZNRecord("testZnode");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.AZURE.name());

    Map<String, String> map = new HashMap<>();
    map.put("addCloudConfig", "true");

    put("clusters/" + clusterName, map,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudID(), "TestCloudID");
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.AZURE.name());

    ClusterConfig clusterConfigFromZk = _configAccessor.getClusterConfig(clusterName);
    Assert.assertEquals(clusterConfigFromZk.getTopology(), AzureConstants.AZURE_TOPOLOGY);
    Assert.assertEquals(clusterConfigFromZk.getFaultZoneType(), AzureConstants.AZURE_FAULT_ZONE_TYPE);
    Assert.assertTrue(clusterConfigFromZk.isTopologyAwareEnabled());
  }

  @Test(dependsOnMethods = "testAddClusterWithCloudConfig")
  public void testAddClusterWithInvalidCloudConfig() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    ZNRecord record = new ZNRecord("testZnode");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");

    Map<String, String> map = new HashMap<>();
    map.put("addCloudConfig", "true");

    // Cloud Provider has not been defined. Result of this rest call will be BAD_REQUEST.
    put("clusters/" + clusterName, map,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test(dependsOnMethods = "testAddClusterWithInvalidCloudConfig")
  public void testAddClusterWithInvalidCustomizedCloudConfig() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    ZNRecord record = new ZNRecord("testZnode");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.CUSTOMIZED.name());

    Map<String, String> map = new HashMap<>();
    map.put("addCloudConfig", "true");

    // Cloud Provider is customized. CLOUD_INFO_PROCESSOR_NAME and CLOUD_INFO_SOURCE fields are
    // required.
    put("clusters/" + clusterName, map,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test(dependsOnMethods = "testAddClusterWithInvalidCustomizedCloudConfig")
  public void testAddClusterWithValidCustomizedCloudConfig() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    ZNRecord record = new ZNRecord("testZnode");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.CUSTOMIZED.name());
    List<String> sourceList = new ArrayList<String>();
    sourceList.add("TestURL");
    record.setListField(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE.name(), sourceList);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(),
        "TestProcessorName");

    Map<String, String> map = new HashMap<>();
    map.put("addCloudConfig", "true");

    // Cloud Provider is customized. CLOUD_INFO_PROCESSOR_NAME and CLOUD_INFO_SOURCE fields are
    // required.
    put("clusters/" + clusterName, map,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudID(), "TestCloudID");
    List<String> listUrlFromZk = cloudConfigFromZk.getCloudInfoSources();
    Assert.assertEquals(listUrlFromZk.get(0), "TestURL");
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoProcessorName(), "TestProcessorName");
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
  }

  @Test(dependsOnMethods = "testAddClusterWithValidCustomizedCloudConfig")
  public void testAddClusterWithCloudConfigDisabledCloud() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    ZNRecord record = new ZNRecord("testZnode");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), false);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.AZURE.name());

    Map<String, String> map = new HashMap<>();
    map.put("addCloudConfig", "true");

    put("clusters/" + clusterName, map,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertFalse(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudID(), "TestCloudID");
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.AZURE.name());
  }


  @Test(dependsOnMethods = "testAddClusterWithCloudConfigDisabledCloud")
  public void testAddCloudConfigNonExistedCluster() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String urlBase = "clusters/TestCloud/cloudconfig/";
    ZNRecord record = new ZNRecord("TestCloud");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.AZURE.name());
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    List<String> testList = new ArrayList<String>();
    testList.add("TestURL");
    record.setListField(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE.name(), testList);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(),
        "TestProcessor");

    // Not found since the cluster is not setup yet.
    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.NOT_FOUND.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddCloudConfigNonExistedCluster")
  public void testAddCloudConfig() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _gSetupTool.addCluster("TestCloud", true);
    String urlBase = "clusters/TestCloud/cloudconfig/";

    ZNRecord record = new ZNRecord("TestCloud");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.CUSTOMIZED.name());
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    List<String> testList = new ArrayList<String>();
    testList.add("TestURL");
    record.setListField(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE.name(), testList);

    // Bad request since Processor has not been defined.
    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());

    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(),
        "TestProcessorName");

    // Now response should be OK since all fields are set
    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig("TestCloud");
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudID(), "TestCloudID");
    List<String> listUrlFromZk = cloudConfigFromZk.getCloudInfoSources();
    Assert.assertEquals(listUrlFromZk.get(0), "TestURL");
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoProcessorName(), "TestProcessorName");
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.CUSTOMIZED.name());

    // Now test the getCloudConfig method.
    String body = get(urlBase, null, Response.Status.OK.getStatusCode(), true);

    ZNRecord recordFromRest = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    CloudConfig cloudConfigRest = new CloudConfig.Builder(recordFromRest).build();
    CloudConfig cloudConfigZk = _configAccessor.getCloudConfig("TestCloud");

    // Check that the CloudConfig from Zk and REST get method are equal
    Assert.assertEquals(cloudConfigRest, cloudConfigZk);

    // Check the fields individually
    Assert.assertTrue(cloudConfigRest.isCloudEnabled());
    Assert.assertEquals(cloudConfigRest.getCloudID(), "TestCloudID");
    Assert.assertEquals(cloudConfigRest.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    List<String> listUrlFromRest = cloudConfigRest.getCloudInfoSources();
    Assert.assertEquals(listUrlFromRest.get(0), "TestURL");
    Assert.assertEquals(cloudConfigRest.getCloudInfoProcessorName(), "TestProcessorName");

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddCloudConfig")
  public void testDeleteCloudConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    ZNRecord record = new ZNRecord("testZnode");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.AZURE.name());

    Map<String, String> map = new HashMap<>();
    map.put("addCloudConfig", "true");
    put("clusters/" + clusterName, map,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());
    // Read CloudConfig from Zookeeper and make sure it has been created
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertNotNull(cloudConfigFromZk);
    String urlBase = "clusters/" + clusterName + "/cloudconfig/";
    delete(urlBase, Response.Status.OK.getStatusCode());

    // Read CloudConfig from Zookeeper and make sure it has been removed
    _configAccessor = new ConfigAccessor(ZK_ADDR);
    cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertNull(cloudConfigFromZk);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }


  @Test(dependsOnMethods = "testDeleteCloudConfig")
  public void testPartialDeleteCloudConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;


    ZNRecord record = new ZNRecord(clusterName);
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.AZURE.name());
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(), "TestProcessor");
    _gSetupTool.addCluster(clusterName, true, new CloudConfig.Builder(record).build());

    String urlBase = "clusters/" + clusterName +"/cloudconfig/";
    Map<String, String> map = new HashMap<>();
    map.put("addCloudConfig", "true");
    put("clusters/" + clusterName, map,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());
    // Read CloudConfig from Zookeeper and make sure it has been created
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertNotNull(cloudConfigFromZk);

    record = new ZNRecord(clusterName);
    Map<String, String> map1 = new HashMap<>();
    map1.put("command",  Command.delete.name());
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(), CloudProvider.AZURE.name());
    post(urlBase, map1, Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Read CloudConfig from Zookeeper and make sure it has been removed
    _configAccessor = new ConfigAccessor(ZK_ADDR);
    cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertNull(cloudConfigFromZk.getCloudID());
    Assert.assertNull(cloudConfigFromZk.getCloudProvider());
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoProcessorName(),"TestProcessor");

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testPartialDeleteCloudConfig")
  public void testUpdateCloudConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _gSetupTool.addCluster("TestCloud", true);
    String urlBase = "clusters/TestCloud/cloudconfig/";

    ZNRecord record = new ZNRecord("TestCloud");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.AZURE.name());

    // Fist add CloudConfig to the cluster
    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Now get the Cloud Config and make sure the information is correct
    String body = get(urlBase, null, Response.Status.OK.getStatusCode(), true);

    ZNRecord recordFromRest = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    CloudConfig cloudConfigRest = new CloudConfig.Builder(recordFromRest).build();
    Assert.assertTrue(cloudConfigRest.isCloudEnabled());
    Assert.assertEquals(cloudConfigRest.getCloudID(), "TestCloudID");
    Assert.assertEquals(cloudConfigRest.getCloudProvider(), CloudProvider.AZURE.name());

    // Now put new information in the ZNRecord
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.CUSTOMIZED.name());
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudIdNew");
    List<String> testList = new ArrayList<String>();
    testList.add("TestURL");
    record.setListField(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE.name(), testList);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(),
        "TestProcessorName");

    Map<String, String> map1 = new HashMap<>();
    map1.put("command", AbstractResource.Command.update.name());

    post(urlBase, map1,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Now get the Cloud Config and make sure the information has been updated
    body = get(urlBase, null, Response.Status.OK.getStatusCode(), true);

    recordFromRest = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    cloudConfigRest = new CloudConfig.Builder(recordFromRest).build();
    Assert.assertTrue(cloudConfigRest.isCloudEnabled());
    Assert.assertEquals(cloudConfigRest.getCloudID(), "TestCloudIdNew");
    Assert.assertEquals(cloudConfigRest.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    List<String> listUrlFromRest = cloudConfigRest.getCloudInfoSources();
    Assert.assertEquals(listUrlFromRest.get(0), "TestURL");
    Assert.assertEquals(cloudConfigRest.getCloudInfoProcessorName(), "TestProcessorName");

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testUpdateCloudConfig")
  public void testAddCustomizedConfigNonExistedCluster() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String urlBase = "clusters/TestCluster/customizedstateconfig/";
    ZNRecord record = new ZNRecord("TestCustomizedStateConfig");
    List<String> testList = new ArrayList<String>();
    testList.add("mockType1");
    record.setListField(
        CustomizedStateConfig.CustomizedStateProperty.AGGREGATION_ENABLED_TYPES
            .name(),
        testList);

    // Expecting not found response since the cluster is not setup yet.
    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.NOT_FOUND.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddCustomizedConfigNonExistedCluster")
  public void testAddCustomizedConfig() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _gSetupTool.addCluster("TestClusterCustomized", true);
    String urlBase = "clusters/TestClusterCustomized/customized-state-config/";
    ZNRecord record = new ZNRecord("TestCustomizedStateConfig");
    List<String> testList = new ArrayList<String>();
    testList.add("mockType1");
    testList.add("mockType2");
    record.setListField(
        CustomizedStateConfig.CustomizedStateProperty.AGGREGATION_ENABLED_TYPES
            .name(),
        testList);

    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Read CustomizedStateConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig customizedConfigFromZk = _configAccessor.getCustomizedStateConfig("TestClusterCustomized");
    List<String> listTypesFromZk = customizedConfigFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(listTypesFromZk.get(0), "mockType1");
    Assert.assertEquals(listTypesFromZk.get(1), "mockType2");

    // Now test the getCustomizedStateConfig method.
    String body = get(urlBase, null, Response.Status.OK.getStatusCode(), true);

    ZNRecord recordFromRest = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    CustomizedStateConfig customizedConfigRest = new CustomizedStateConfig.Builder(recordFromRest).build();
    CustomizedStateConfig customizedConfigZk = _configAccessor.getCustomizedStateConfig("TestClusterCustomized");

    // Check that the CustomizedStateConfig from Zk and REST get method are equal
    Assert.assertEquals(customizedConfigRest, customizedConfigZk);

    // Check the fields individually
    List<String> listUrlFromRest = customizedConfigRest.getAggregationEnabledTypes();
    Assert.assertEquals(listUrlFromRest.get(0), "mockType1");
    Assert.assertEquals(listUrlFromRest.get(1), "mockType2");

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddCustomizedConfig")
  public void testDeleteCustomizedConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _gSetupTool.addCluster("TestClusterCustomized", true);
    String urlBase = "clusters/TestClusterCustomized/customized-state-config/";
    ZNRecord record = new ZNRecord("TestCustomizedStateConfig");
    List<String> testList = new ArrayList<String>();
    testList.add("mockType1");
    record.setListField(
        CustomizedStateConfig.CustomizedStateProperty.AGGREGATION_ENABLED_TYPES
            .name(),
        testList);

    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Read CustomizedStateConfig from Zookeeper and make sure it exists
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig customizedConfigFromZk = _configAccessor.getCustomizedStateConfig("TestClusterCustomized");
    Assert.assertNotNull(customizedConfigFromZk);

    delete(urlBase, Response.Status.OK.getStatusCode());

    customizedConfigFromZk = _configAccessor.getCustomizedStateConfig("TestClusterCustomized");
    Assert.assertNull(customizedConfigFromZk);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testDeleteCustomizedConfig")
  public void testUpdateCustomizedConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _gSetupTool.addCluster("TestClusterCustomized", true);
    String urlBase = "clusters/TestClusterCustomized/customized-state-config/";
    ZNRecord record = new ZNRecord("TestCustomizedStateConfig");
    List<String> testList = new ArrayList<String>();
    testList.add("mockType1");
    record.setListField(
        CustomizedStateConfig.CustomizedStateProperty.AGGREGATION_ENABLED_TYPES
            .name(),
        testList);

    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Read CustomizedStateConfig from Zookeeper and make sure it exists
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig customizedConfigFromZk = _configAccessor.getCustomizedStateConfig("TestClusterCustomized");
    Assert.assertNotNull(customizedConfigFromZk);

    // Add new type to CustomizedStateConfig
    Map<String, String> map1 = new HashMap<>();
    map1.put("command", Command.add.name());
    map1.put("type", "mockType2");

    post(urlBase, map1, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    customizedConfigFromZk =
        _configAccessor.getCustomizedStateConfig("TestClusterCustomized");
    List<String> listTypesFromZk = customizedConfigFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(listTypesFromZk.get(0), "mockType1");
    Assert.assertEquals(listTypesFromZk.get(1), "mockType2");

    // Remove a type to CustomizedStateConfig
    Map<String, String> map2 = new HashMap<>();
    map2.put("command", Command.delete.name());
    map2.put("type", "mockType1");

    post(urlBase, map2, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    customizedConfigFromZk =
        _configAccessor.getCustomizedStateConfig("TestClusterCustomized");
    listTypesFromZk = customizedConfigFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(listTypesFromZk.get(0), "mockType2");
    Assert.assertFalse(listTypesFromZk.contains("mockType1"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private ClusterConfig getClusterConfigFromRest(String cluster) throws IOException {
    String body = get("clusters/" + cluster + "/configs", null, Response.Status.OK.getStatusCode(), true);

    ZNRecord record = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    ClusterConfig clusterConfigRest = new ClusterConfig(record);
    ClusterConfig clusterConfigZk = _configAccessor.getClusterConfig(cluster);
    Assert.assertEquals(clusterConfigZk, clusterConfigRest, "cluster config from response: "
        + clusterConfigRest + " vs cluster config actually: " + clusterConfigZk);

    return clusterConfigRest;
  }

  private void updateClusterConfigFromRest(String cluster, ClusterConfig newConfig, Command command)
      throws IOException {
    _auditLogger.clearupLogs();
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(newConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + cluster + "/configs", ImmutableMap.of("command", command.name()), entity,
        Response.Status.OK.getStatusCode());

    Assert.assertEquals(_auditLogger.getAuditLogs().size(), 1);
    AuditLog auditLog = _auditLogger.getAuditLogs().get(0);
    validateAuditLog(auditLog, HTTPMethods.POST.name(), "clusters/" + cluster + "/configs",
        Response.Status.OK.getStatusCode(), null);
  }

  private ClusterConfig createClusterConfig(String cluster) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(cluster);

    clusterConfig.setPersistBestPossibleAssignment(true);
    clusterConfig.getRecord().setSimpleField("SimpleField1", "Value1");
    clusterConfig.getRecord().setSimpleField("SimpleField2", "Value2");

    clusterConfig.getRecord().setListField("ListField1",
        Arrays.asList("Value1", "Value2", "Value3"));
    clusterConfig.getRecord().setListField("ListField2",
        Arrays.asList("Value2", "Value1", "Value3"));

    clusterConfig.getRecord().setMapField("MapField1", new HashMap<String, String>() {
      {
        put("key1", "value1");
        put("key2", "value2");
      }
    });
    clusterConfig.getRecord().setMapField("MapField2", new HashMap<String, String>() {
      {
        put("key3", "value1");
        put("key4", "value2");
      }
    });

    return clusterConfig;
  }

  private void validateAuditLog(AuditLog auditLog, String httpMethod, String requestPath,
      int statusCode, String responseEntity) {
    Assert.assertEquals(auditLog.getHttpMethod(), httpMethod);
    Assert.assertNotNull(auditLog.getClientIP());
    Assert.assertNotNull(auditLog.getClientHostPort());
    Assert.assertNotNull(auditLog.getCompleteTime());
    Assert.assertNotNull(auditLog.getStartTime());
    Assert.assertEquals(auditLog.getNamespace(), HelixRestNamespace.DEFAULT_NAMESPACE_NAME);
    Assert.assertEquals(auditLog.getRequestPath(), requestPath);
    Assert.assertEquals(auditLog.getResponseCode(), statusCode);
    Assert.assertEquals(auditLog.getResponseEntity(), responseEntity);
  }
}
