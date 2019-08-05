package org.apache.helix.rest.server;

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
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestInstancesAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";

  @Test
  public void testInstancesStoppable_zoneBased() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Select instances with zone based
    String content =
        String.format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
            "instance2", "instance3", "instance4", "instance5", "invalidInstance");
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances?command=stoppable")
        .format(STOPPABLE_CLUSTER)
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    String checkResult = response.readEntity(String.class);
    Assert.assertEquals(checkResult, "{\n" + "  \"instance_stoppable_parallel\" : [ ],\n"
        + "  \"instance_not_stoppable_with_reasons\" : {\n"
        + "    \"instance0\" : [ \"Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED\" ],\n"
        + "    \"instance1\" : [ \"Helix:EMPTY_RESOURCE_ASSIGNMENT\", \"Helix:INSTANCE_NOT_ENABLED\", \"Helix:INSTANCE_NOT_STABLE\" ],\n"
        + "    \"instance2\" : [ \"Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED\" ],\n"
        + "    \"instance3\" : [ \"Helix:HAS_DISABLED_PARTITION\", \"Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED\" ],\n"
        + "    \"instance4\" : [ \"Helix:EMPTY_RESOURCE_ASSIGNMENT\", \"Helix:INSTANCE_NOT_ALIVE\", \"Helix:INSTANCE_NOT_STABLE\" ],\n"
        + "    \"invalidInstance\" : [ \"Helix:INSTANCE_NOT_EXIST\" ]\n"
        + "  }\n" + "}\n");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstancesStoppable_zoneBased")
  public void testInstancesStoppable_disableOneInstance() throws InterruptedException {
    // Disable one selected instance0, it should failed to check
    String instance = "instance0";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER, instance);
    instanceConfig.setInstanceEnabled(false);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", false);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);

    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    Entity entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    String checkResult = response.readEntity(String.class);
    Assert.assertEquals(checkResult,
        "{\"stoppable\":false,\"failedChecks\":[\"Helix:HAS_DISABLED_PARTITION\",\"Helix:INSTANCE_NOT_ENABLED\",\"Helix:INSTANCE_NOT_STABLE\",\"Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED\"]}");

    // Reenable instance0, it should passed the check
    instanceConfig.setInstanceEnabled(true);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", true);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);
    Assert.assertTrue(verifier.verifyByPolling());

    entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    checkResult = response.readEntity(String.class);
    Assert.assertEquals(checkResult,
        "{\"stoppable\":false,\"failedChecks\":[\"Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED\"]}");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstancesStoppable_disableOneInstance")
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
    System.out.println("End test :" + TestHelper.getTestMethodName());
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
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}
