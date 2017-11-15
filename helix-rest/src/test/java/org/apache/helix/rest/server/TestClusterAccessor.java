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

import com.google.common.collect.ImmutableMap;
import com.sun.research.ws.wadl.HTTPMethods;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.rest.server.resources.AbstractResource.Command;
import org.apache.helix.rest.server.resources.ClusterAccessor;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterAccessor extends AbstractTestClass {
  ObjectMapper _mapper = new ObjectMapper();

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
    String body = get("clusters", Response.Status.OK.getStatusCode(), true);
    JsonNode node = _mapper.readTree(body);
    String clustersStr = node.get(ClusterAccessor.ClusterProperties.clusters.name()).toString();
    Assert.assertNotNull(clustersStr);

    Set<String> clusters = _mapper.readValue(clustersStr,
        _mapper.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(clusters, _clusters,
        "clusters from response: " + clusters + " vs clusters actually: " + _clusters);

    Assert.assertEquals(_auditLogger.getAuditLogs().size(), 1);
    AuditLog auditLog = _auditLogger.getAuditLogs().get(0);
    validateAuditLog(auditLog, HTTPMethods.GET.name(), "clusters",
        Response.Status.OK.getStatusCode(), body);
  }

  @Test(dependsOnMethods = "testGetClusters")
  public void testAddConfigFields() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    ClusterConfig oldConfig = getClusterConfigFromRest(cluster);

    ClusterConfig configDelta = new ClusterConfig(cluster);
    configDelta.getRecord().setSimpleField("newField", "newValue");
    configDelta.getRecord().setListField("newList", Arrays.asList("newValue1", "newValue2"));
    configDelta.getRecord().setMapField("newMap", new HashMap<String, String>() {{
      put("newkey1", "newvalue1");
      put("newkey2", "newvalue2");
    }});

    updateClusterConfigFromRest(cluster, configDelta, Command.update);

    ClusterConfig newConfig = getClusterConfigFromRest(cluster);
    oldConfig.getRecord().update(configDelta.getRecord());
    Assert.assertEquals(newConfig, oldConfig,
        "cluster config from response: " + newConfig + " vs cluster config actually: " + oldConfig);
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
    Assert.assertEquals(newConfig, prevConfig,
        "cluster config from response: " + newConfig + " vs cluster config actually: " + prevConfig);
  }

  @Test (dependsOnMethods = "testUpdateConfigFields")
  public void testDeleteConfigFields()
      throws IOException {
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
    Assert.assertEquals(newConfig, prevConfig,
        "cluster config from response: " + newConfig + " vs cluster config actually: "
            + prevConfig);
  }

  @Test(dependsOnMethods = "testDeleteConfigFields")
  public void testCreateDeleteCluster() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // create an existing cluster should fail.
    _auditLogger.clearupLogs();
    String cluster = _clusters.iterator().next();
    put("clusters/" + cluster, null, Entity.entity(new String(), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // create a new cluster
    cluster = "NewCluster";
    put("clusters/" + cluster, null, Entity.entity(new String(), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // verify the cluster has been created.
    Assert.assertTrue(ZKUtil.isClusterSetup(cluster, _gZkClient));

    // delete the cluster
    delete("clusters/" + cluster, Response.Status.OK.getStatusCode());

    // verify the cluster has been deleted.
    Assert.assertFalse(_baseAccessor.exists("/" + cluster, 0));
    Assert.assertEquals(_auditLogger.getAuditLogs().size(), 3);
  }

  @Test(dependsOnMethods = "testCreateDeleteCluster")
  public void testEnableDisableCluster() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // disable a cluster.
    String cluster = _clusters.iterator().next();
    _auditLogger.clearupLogs();
    post("clusters/" + cluster, ImmutableMap.of("command", "disable"),
        Entity.entity(new String(), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(cluster);
    // verify the cluster is paused.
    Assert.assertTrue(_baseAccessor.exists(keyBuilder.pause().getPath(), 0));

    // enable a cluster.
    post("clusters/" + cluster, ImmutableMap.of("command", "enable"),
        Entity.entity(new String(), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // verify the cluster is paused.
    Assert.assertFalse(_baseAccessor.exists(keyBuilder.pause().getPath(), 0));
    Assert.assertEquals(_auditLogger.getAuditLogs().size(), 2);
  }

  @Test(dependsOnMethods = "testEnableDisableCluster")
  public void testGetClusterConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Response response = target("clusters/fakeCluster/configs").request().get();
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    String cluster = _clusters.iterator().next();
    getClusterConfigFromRest(cluster);
  }

  @Test(dependsOnMethods = "testGetClusterConfig")
  public void testEnableDisableMaintenanceMode() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String cluster = _clusters.iterator().next();
    String reason = "Test reason";
    HelixDataAccessor accessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    post("clusters/" + cluster, ImmutableMap.of("command", "enableMaintenanceMode"),
        Entity.entity(reason, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    MaintenanceSignal signal = accessor.getProperty(accessor.keyBuilder().maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(reason, signal.getReason());
    post("clusters/" + cluster, ImmutableMap.of("command", "disableMaintenanceMode"),
        Entity.entity(new String(), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    Assert.assertNull(accessor.getProperty(accessor.keyBuilder().maintenance()));
  }

  private ClusterConfig getClusterConfigFromRest(String cluster) throws IOException {
    String body = get("clusters/" + cluster + "/configs", Response.Status.OK.getStatusCode(), true);

    ZNRecord record = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    ClusterConfig clusterConfigRest = new ClusterConfig(record);
    ClusterConfig clusterConfigZk = _configAccessor.getClusterConfig(cluster);
    Assert.assertEquals(clusterConfigZk, clusterConfigRest,
        "cluster config from response: " + clusterConfigRest + " vs cluster config actually: "
            + clusterConfigZk);

    return clusterConfigRest;
  }

  private void updateClusterConfigFromRest(String cluster, ClusterConfig newConfig,
      Command command) throws IOException {
    _auditLogger.clearupLogs();
    Entity entity = Entity
        .entity(_mapper.writeValueAsString(newConfig.getRecord()), MediaType.APPLICATION_JSON_TYPE);
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

    clusterConfig.getRecord()
        .setListField("ListField1", Arrays.asList("Value1", "Value2", "Value3"));
    clusterConfig.getRecord()
        .setListField("ListField2", Arrays.asList("Value2", "Value1", "Value3"));

    clusterConfig.getRecord().setMapField("MapField1", new HashMap<String, String>() {{
      put("key1", "value1");
      put("key2", "value2");
    }});
    clusterConfig.getRecord().setMapField("MapField2", new HashMap<String, String>() {{
      put("key3", "value1");
      put("key4", "value2");
    }});

    return clusterConfig;
  }

  private void validateAuditLog(AuditLog auditLog, String httpMethod, String requestPath,
      int statusCode, String responseEntity) {
    Assert.assertEquals(auditLog.getHttpMethod(), httpMethod);
    Assert.assertNotNull(auditLog.getClientIP());
    Assert.assertNotNull(auditLog.getClientHostPort());
    Assert.assertNotNull(auditLog.getCompleteTime());
    Assert.assertNotNull(auditLog.getStartTime());
    Assert.assertEquals(auditLog.getRequestPath(), requestPath);
    Assert.assertEquals(auditLog.getResponseCode(), statusCode);
    Assert.assertEquals(auditLog.getResponseEntity(), responseEntity);
  }
}
