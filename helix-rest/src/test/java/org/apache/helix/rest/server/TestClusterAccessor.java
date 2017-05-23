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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.rest.server.resources.ClusterAccessor;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.BeforeClass;
import org.testng.Assert;
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
    final Response response = target("clusters").request().get();
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertEquals(response.getMediaType().getType(), "application");

    String body = response.readEntity(String.class);
    Assert.assertNotNull(body);

    JsonNode node = _mapper.readTree(body);
    String clustersStr = node.get(ClusterAccessor.ClusterProperties.clusters.name()).toString();
    Assert.assertNotNull(clustersStr);

    Set<String> clusters = _mapper.readValue(clustersStr,
        _mapper.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(clusters, _clusters,
        "clusters from response: " + clusters + " vs clusters actually: " + _clusters);
  }

  @Test
  public void testAddConfigFields() throws IOException {
    String cluster = _clusters.iterator().next();
    ClusterConfig oldConfig = getClusterConfigFromRest(cluster);

    ClusterConfig configDelta = new ClusterConfig(cluster);
    configDelta.getRecord().setSimpleField("newField", "newValue");
    configDelta.getRecord().setListField("newList", Arrays.asList("newValue1", "newValue2"));
    configDelta.getRecord().setMapField("newMap", new HashMap<String, String>() {{
      put("newkey1", "newvalue1");
      put("newkey2", "newvalue2");
    }});

    updateClusterConfigFromRest(cluster, configDelta);

    ClusterConfig newConfig = getClusterConfigFromRest(cluster);
    oldConfig.getRecord().update(configDelta.getRecord());
    Assert.assertEquals(newConfig, oldConfig,
        "cluster config from response: " + newConfig + " vs cluster config actually: " + oldConfig);
  }

  @Test
  public void testUpdateConfigFields() throws IOException {
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
    updateClusterConfigFromRest(cluster, config);
    ClusterConfig newConfig = getClusterConfigFromRest(cluster);

    prevConfig.getRecord().update(config.getRecord());
    Assert.assertEquals(newConfig, prevConfig,
        "cluster config from response: " + newConfig + " vs cluster config actually: " + prevConfig);
  }


  @Test
  public void testGetClusterConfig() throws IOException {
    Response response = target("clusters/fakeCluster/configs").request().get();
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    String cluster = _clusters.iterator().next();
    getClusterConfigFromRest(cluster);
  }

  private ClusterConfig getClusterConfigFromRest(String cluster) throws IOException {
    Response response = target("clusters/" + cluster + "/configs").request().get();
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertEquals(response.getMediaType().getType(), "application");
    Assert.assertEquals(response.getMediaType().getSubtype(), "json");

    String body = response.readEntity(String.class);
    Assert.assertNotNull(body);
    ZNRecord record = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    ClusterConfig clusterConfigRest = new ClusterConfig(record);
    ClusterConfig clusterConfigZk = _configAccessor.getClusterConfig(cluster);
    Assert.assertEquals(clusterConfigZk, clusterConfigRest,
        "cluster config from response: " + clusterConfigRest + " vs cluster config actually: "
            + clusterConfigZk);

    return clusterConfigRest;
  }

  private void updateClusterConfigFromRest(String cluster, ClusterConfig newConfig)
      throws IOException {
    Entity entity =
        Entity.entity(_mapper.writeValueAsString(newConfig.getRecord()), MediaType.APPLICATION_JSON_TYPE);
    Response response = target("clusters/" + cluster + "/configs").request().post(entity);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
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
}
