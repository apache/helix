package org.apache.helix.rest.server;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.TestHelper;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestInstancesAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";

  @Test
  public void testEndToEndChecks() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Select instances with zone based
    String content = String
        .format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
            "instance2", "instance3", "instance4", "instance5");
    Response response =
        new JerseyUriRequestBuilder("clusters/{}/instances?command=stoppable").format(STOPPABLE_CLUSTER)
            .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    String checkResult = response.readEntity(String.class);
    Assert.assertEquals(checkResult,
        "{\n  \"instance_stoppable_parallel\" : [ \"instance0\", \"instance2\" ],\n"
            + "  \"instance_not_stoppable_with_reasons\" : {\n    \"instance1\" : [ \"Helix:EMPTY_RESOURCE_ASSIGNMENT\", \"Helix:INSTANCE_NOT_ENABLED\", \"Helix:INSTANCE_NOT_STABLE\" ],\n"
            + "    \"instance3\" : [ \"Helix:HAS_DISABLED_PARTITION\" ],\n"
            + "    \"instance4\" : [ \"Helix:EMPTY_RESOURCE_ASSIGNMENT\", \"Helix:INSTANCE_NOT_STABLE\", \"Helix:INSTANCE_NOT_ALIVE\" ]\n  }\n}\n");

    // Disable one selected instance0, it should failed to check
    String instance = "instance0";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER, instance);
    instanceConfig.setInstanceEnabled(false);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", false);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);

    Entity entity =
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    checkResult = response.readEntity(String.class);
    Assert.assertEquals(checkResult,
        "{\"stoppable\":false,\"failedChecks\":[\"Helix:HAS_DISABLED_PARTITION\",\"Helix:INSTANCE_NOT_ENABLED\",\"Helix:INSTANCE_NOT_STABLE\"]}");

    // Reenable instance0, it should passed the check
    instanceConfig.setInstanceEnabled(true);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", true);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);
    HelixClusterVerifier
        verifier = new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(((BestPossibleExternalViewVerifier) verifier).verifyByPolling());

    entity =
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    checkResult = response.readEntity(String.class);
    Assert.assertEquals(checkResult,
        "{\"stoppable\":true,\"failedChecks\":[]}");
  }

  @Test(dependsOnMethods = "testEndToEndChecks")
  public void testGetAllInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances").isBodyReturnExpected(true)
        .format(CLUSTER_NAME).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesStr = node.get(InstancesAccessor.InstancesProperties.instances.name()).toString();
    Assert.assertNotNull(instancesStr);

    Set<String> instances = OBJECT_MAPPER.readValue(instancesStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(instances, _instancesMap.get(CLUSTER_NAME), "Instances from response: "
        + instances + " vs instances actually: " + _instancesMap.get(CLUSTER_NAME));
  }

  @Test(enabled = false)
  public void testUpdateInstances() throws IOException {
    // TODO: Reenable the test after storage node fix the problem
    // Batch disable instances

    List<String> instancesToDisable = Arrays.asList(
        new String[] { CLUSTER_NAME + "localhost_12918", CLUSTER_NAME + "localhost_12919",
            CLUSTER_NAME + "localhost_12920"
        });
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(
        ImmutableMap.of(InstancesAccessor.InstancesProperties.instances.name(), instancesToDisable)),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/instances", ImmutableMap.of("command", "disable"), entity,
        Response.Status.OK.getStatusCode());
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertEquals(clusterConfig.getDisabledInstances().keySet(),
        new HashSet<>(instancesToDisable));

    instancesToDisable = Arrays
        .asList(new String[] { CLUSTER_NAME + "localhost_12918", CLUSTER_NAME + "localhost_12920"
        });
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(
        ImmutableMap.of(InstancesAccessor.InstancesProperties.instances.name(), instancesToDisable)),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/instances", ImmutableMap.of("command", "enable"), entity,
        Response.Status.OK.getStatusCode());
    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertEquals(clusterConfig.getDisabledInstances().keySet(),
        new HashSet<>(Arrays.asList(CLUSTER_NAME + "localhost_12919")));
  }
}
