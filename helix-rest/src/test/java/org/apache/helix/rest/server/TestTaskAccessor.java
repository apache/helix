package org.apache.helix.rest.server;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.TestHelper;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestTaskAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";

  @Test
  public void testGetAddTaskUserContent() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String uri = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/Job_0/tasks/0/userContent";
    String uriTaskDoesNotExist = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/Job_0/tasks/xxx/userContent";

    // Empty user content
    String body =
        get(uri, Response.Status.OK.getStatusCode(), true);
    Map<String, String>
        contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {});
    Assert.assertTrue(contentStore.isEmpty());

    // Post user content
    Map<String, String> map1 = new HashMap<>();
    map1.put("k1", "v1");
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(map1), MediaType.APPLICATION_JSON_TYPE);
    post(uri, ImmutableMap.of("command", "update"), entity, Response.Status.OK.getStatusCode());
    post(uriTaskDoesNotExist, ImmutableMap.of("command", "update"), entity, Response.Status.OK.getStatusCode());

    // get after post should work
    body = get(uri, Response.Status.OK.getStatusCode(), true);
    contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {
    });
    Assert.assertEquals(contentStore, map1);
    body = get(uriTaskDoesNotExist, Response.Status.OK.getStatusCode(), true);
    contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {
    });
    Assert.assertEquals(contentStore, map1);


    // modify map1 and verify
    map1.put("k1", "v2");
    map1.put("k2", "v2");
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(map1), MediaType.APPLICATION_JSON_TYPE);
    post(uri, ImmutableMap.of("command", "update"), entity, Response.Status.OK.getStatusCode());
    body = get(uri, Response.Status.OK.getStatusCode(), true);
    contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {
    });
    Assert.assertEquals(contentStore, map1);
  }

  @Test
  public void testInvalidGetAddTaskUserContent() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String validURI = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/Job_0/tasks/0/userContent";
    String invalidURI1 = "clusters/" + CLUSTER_NAME + "/workflows/xxx/jobs/Job_0/tasks/0/userContent"; // workflow not exist
    String invalidURI2 = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/xxx/tasks/0/userContent"; // job not exist
    String invalidURI3 = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/xxx/tasks/xxx/userContent"; // task not exist
    Entity validEntity = Entity.entity("{\"k1\":\"v1\"}", MediaType.APPLICATION_JSON_TYPE);
    Entity invalidEntity = Entity.entity("{\"k1\":{}}", MediaType.APPLICATION_JSON_TYPE); // not Map<String, String>
    Map<String, String> validCmd = ImmutableMap.of("command", "update");
    Map<String, String> invalidCmd = ImmutableMap.of("command", "delete"); // cmd not supported

    get(invalidURI1, Response.Status.NOT_FOUND.getStatusCode(), false);
    get(invalidURI2, Response.Status.NOT_FOUND.getStatusCode(), false);
    get(invalidURI3, Response.Status.NOT_FOUND.getStatusCode(), false);

    post(invalidURI1, validCmd, validEntity, Response.Status.NOT_FOUND.getStatusCode());
    post(invalidURI2, validCmd, validEntity, Response.Status.NOT_FOUND.getStatusCode());

    post(validURI, invalidCmd, validEntity, Response.Status.BAD_REQUEST.getStatusCode());
    post(validURI, validCmd, invalidEntity, Response.Status.BAD_REQUEST.getStatusCode());
  }
}
