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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.rest.server.resources.AbstractResource.Command;
import org.apache.helix.rest.server.resources.helix.ClusterAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.sun.research.ws.wadl.HTTPMethods;

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
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddConfigFields")
  public void testUpdateConfigFields() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    ClusterConfig config = getClusterConfigFromRest(cluster);

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
        node.get(ClusterAccessor.ClusterProperties.maintenance.name()).getBooleanValue();
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
        node.get(ClusterAccessor.ClusterProperties.maintenance.name()).getBooleanValue());

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
    Assert.assertEquals(stateModelNames.size(), 6);

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
