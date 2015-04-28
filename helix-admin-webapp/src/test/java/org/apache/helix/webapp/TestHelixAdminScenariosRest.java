package org.apache.helix.webapp;

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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.apache.helix.webapp.resources.ClusterRepresentationUtil;
import org.apache.helix.webapp.resources.InstancesResource.ListInstancesWrapper;
import org.apache.helix.webapp.resources.JsonParameters;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.restlet.Component;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Simulate all the admin tasks needed by using command line tool
 */
public class TestHelixAdminScenariosRest extends AdminTestBase {
  private static final int MAX_RETRIES = 5;

  RestAdminApplication _adminApp;
  Component _component;
  String _tag1 = "tag1123";
  String _tag2 = "tag212334";

  public static String ObjectToJson(Object object) throws JsonGenerationException,
      JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, object);

    return sw.toString();
  }

  public static <T extends Object> T JsonToObject(Class<T> clazz, String jsonString)
      throws JsonParseException, JsonMappingException, IOException {
    StringReader sr = new StringReader(jsonString);
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(sr, clazz);
  }

  static String assertSuccessPostOperation(String url, Map<String, String> jsonParameters,
      boolean hasException) throws IOException {
    return assertSuccessPostOperation(url, jsonParameters, null, hasException);
  }

  static String assertSuccessPostOperation(String url, Map<String, String> jsonParameters,
      Map<String, String> extraForm, boolean hasException) throws IOException {
    Reference resourceRef = new Reference(url);

    int numRetries = 0;
    while (numRetries <= MAX_RETRIES) {
      Request request = new Request(Method.POST, resourceRef);

      if (extraForm != null) {
        String entity =
            JsonParameters.JSON_PARAMETERS + "="
                + ClusterRepresentationUtil.ObjectToJson(jsonParameters);
        for (String key : extraForm.keySet()) {
          entity = entity + "&" + (key + "=" + extraForm.get(key));
        }
        request.setEntity(entity, MediaType.APPLICATION_ALL);
      } else {
        request
            .setEntity(
                JsonParameters.JSON_PARAMETERS + "="
                    + ClusterRepresentationUtil.ObjectToJson(jsonParameters),
                MediaType.APPLICATION_ALL);
      }

      Response response = _gClient.handle(request);
      Representation result = response.getEntity();
      StringWriter sw = new StringWriter();

      if (result != null) {
        result.write(sw);
      }

      int code = response.getStatus().getCode();
      boolean successCode =
          code == Status.SUCCESS_NO_CONTENT.getCode() || code == Status.SUCCESS_OK.getCode();
      if (successCode || numRetries == MAX_RETRIES) {
        Assert.assertTrue(successCode);
        Assert.assertTrue(hasException == sw.toString().toLowerCase().contains("exception"));
        return sw.toString();
      }
      numRetries++;
    }
    Assert.fail("Request failed after all retries");
    return null;
  }

  void deleteUrl(String url, boolean hasException) throws IOException {
    Reference resourceRef = new Reference(url);
    Request request = new Request(Method.DELETE, resourceRef);
    Response response = _gClient.handle(request);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);
    Assert.assertTrue(hasException == sw.toString().toLowerCase().contains("exception"));
  }

  String getUrl(String url) throws IOException {
    Reference resourceRef = new Reference(url);
    Request request = new Request(Method.GET, resourceRef);
    Response response = _gClient.handle(request);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);
    return sw.toString();
  }

  String getClusterUrl(String cluster) {
    return "http://localhost:" + ADMIN_PORT + "/clusters" + "/" + cluster;
  }

  String getInstanceUrl(String cluster, String instance) {
    return "http://localhost:" + ADMIN_PORT + "/clusters/" + cluster + "/instances/" + instance;
  }

  String getResourceUrl(String cluster, String resourceGroup) {
    return "http://localhost:" + ADMIN_PORT + "/clusters/" + cluster + "/resourceGroups/"
        + resourceGroup;
  }

  void assertClusterSetupException(String command) {
    boolean exceptionThrown = false;
    try {
      ClusterSetup.processCommandLineArgs(command.split(" "));
    } catch (Exception e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  private Map<String, String> addClusterCmd(String clusterName) {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.CLUSTER_NAME, clusterName);
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addCluster);

    return parameters;
  }

  private void addCluster(String clusterName) throws IOException {
    String url = "http://localhost:" + ADMIN_PORT + "/clusters";
    String response = assertSuccessPostOperation(url, addClusterCmd(clusterName), false);
    Assert.assertTrue(response.contains(clusterName));
  }

  @Test
  public void testAddCluster() throws Exception {
    String url = "http://localhost:" + ADMIN_PORT + "/clusters";

    // Normal add
    String response = assertSuccessPostOperation(url, addClusterCmd("clusterTest"), false);
    Assert.assertTrue(response.contains("clusterTest"));

    // malformed cluster name
    response = assertSuccessPostOperation(url, addClusterCmd("/ClusterTest"), true);

    // Add the grand cluster
    response = assertSuccessPostOperation(url, addClusterCmd("Klazt3rz"), false);
    Assert.assertTrue(response.contains("Klazt3rz"));

    response = assertSuccessPostOperation(url, addClusterCmd("\\ClusterTest"), false);
    Assert.assertTrue(response.contains("\\ClusterTest"));

    // Add already exist cluster
    response = assertSuccessPostOperation(url, addClusterCmd("clusterTest"), true);

    // delete cluster without resource and instance
    Assert.assertTrue(ZKUtil.isClusterSetup("Klazt3rz", _zkclient));
    Assert.assertTrue(ZKUtil.isClusterSetup("clusterTest", _zkclient));
    Assert.assertTrue(ZKUtil.isClusterSetup("\\ClusterTest", _zkclient));

    String clusterUrl = getClusterUrl("\\ClusterTest");
    deleteUrl(clusterUrl, false);

    String clustersUrl = "http://localhost:" + ADMIN_PORT + "/clusters";
    response = getUrl(clustersUrl);

    clusterUrl = getClusterUrl("clusterTest1");
    deleteUrl(clusterUrl, false);
    response = getUrl(clustersUrl);
    Assert.assertFalse(response.contains("clusterTest1"));

    clusterUrl = getClusterUrl("clusterTest");
    deleteUrl(clusterUrl, false);
    response = getUrl(clustersUrl);
    Assert.assertFalse(response.contains("clusterTest"));

    clusterUrl = getClusterUrl("clusterTestOK");
    deleteUrl(clusterUrl, false);

    Assert.assertFalse(_zkclient.exists("/clusterTest"));
    Assert.assertFalse(_zkclient.exists("/clusterTest1"));
    Assert.assertFalse(_zkclient.exists("/clusterTestOK"));

    response = assertSuccessPostOperation(url, addClusterCmd("clusterTest1"), false);
    response = getUrl(clustersUrl);
    Assert.assertTrue(response.contains("clusterTest1"));
  }

  private Map<String, String> addResourceCmd(String resourceName, String stateModelDef,
      int partition) {
    Map<String, String> parameters = new HashMap<String, String>();

    parameters.put(JsonParameters.RESOURCE_GROUP_NAME, resourceName);
    parameters.put(JsonParameters.STATE_MODEL_DEF_REF, stateModelDef);
    parameters.put(JsonParameters.PARTITIONS, "" + partition);
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addResource);

    return parameters;
  }

  private void addResource(String clusterName, String resourceName, int partitions)
      throws IOException {
    final String reourcesUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/resourceGroups";
    String response =
        assertSuccessPostOperation(reourcesUrl,
            addResourceCmd(resourceName, "MasterSlave", partitions), false);
    Assert.assertTrue(response.contains(resourceName));
  }

  @Test
  public void testAddResource() throws Exception {
    final String clusterName = "clusterTestAddResource";
    addCluster(clusterName);

    String reourcesUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/resourceGroups";
    String response =
        assertSuccessPostOperation(reourcesUrl, addResourceCmd("db_22", "MasterSlave", 144), false);
    Assert.assertTrue(response.contains("db_22"));

    response =
        assertSuccessPostOperation(reourcesUrl, addResourceCmd("db_11", "MasterSlave", 44), false);
    Assert.assertTrue(response.contains("db_11"));

    // Add duplicate resource
    response =
        assertSuccessPostOperation(reourcesUrl, addResourceCmd("db_22", "OnlineOffline", 55), true);

    // drop resource now
    String resourceUrl = getResourceUrl(clusterName, "db_11");
    deleteUrl(resourceUrl, false);
    Assert.assertFalse(_zkclient.exists("/" + clusterName + "/IDEALSTATES/db_11"));

    response =
        assertSuccessPostOperation(reourcesUrl, addResourceCmd("db_11", "MasterSlave", 44), false);
    Assert.assertTrue(response.contains("db_11"));

    Assert.assertTrue(_zkclient.exists("/" + clusterName + "/IDEALSTATES/db_11"));

    response =
        assertSuccessPostOperation(reourcesUrl, addResourceCmd("db_33", "MasterSlave", 44), false);
    Assert.assertTrue(response.contains("db_33"));

    response =
        assertSuccessPostOperation(reourcesUrl, addResourceCmd("db_44", "MasterSlave", 44), false);
    Assert.assertTrue(response.contains("db_44"));
  }

  private Map<String, String> activateClusterCmd(String grandClusterName, boolean enabled) {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.GRAND_CLUSTER, grandClusterName);
    parameters.put(JsonParameters.ENABLED, "" + enabled);
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.activateCluster);

    return parameters;
  }

  @Test
  public void testDeactivateCluster() throws Exception {
    final String clusterName = "clusterTestDeactivateCluster";
    final String controllerClusterName = "controllerClusterTestDeactivateCluster";

    Map<String, MockParticipant> participants =
        new HashMap<String, MockParticipant>();
    Map<String, MockMultiClusterController> multiClusterControllers =
        new HashMap<String, MockMultiClusterController>();

    // setup cluster
    addCluster(clusterName);
    addInstancesToCluster(clusterName, "localhost:123", 6, null);
    addResource(clusterName, "db_11", 16);
    rebalanceResource(clusterName, "db_11");

    addCluster(controllerClusterName);
    addInstancesToCluster(controllerClusterName, "controller_900", 2, null);

    // start mock nodes
    for (int i = 0; i < 6; i++) {
      String instanceName = "localhost_123" + i;
      MockParticipant participant =
          new MockParticipant(_zkaddr, clusterName, instanceName);
      participant.syncStart();
      participants.put(instanceName, participant);
    }

    // start controller nodes
    for (int i = 0; i < 2; i++) {
      String controllerName = "controller_900" + i;
      MockMultiClusterController multiClusterController =
          new MockMultiClusterController(_zkaddr, controllerClusterName, controllerName);
      multiClusterController.syncStart();
      multiClusterControllers.put(controllerName, multiClusterController);
    }

    String clusterUrl = getClusterUrl(clusterName);

    // activate cluster
    assertSuccessPostOperation(clusterUrl, activateClusterCmd(controllerClusterName, true), false);
    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            controllerClusterName));
    Assert.assertTrue(verifyResult);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    // deactivate cluster
    assertSuccessPostOperation(clusterUrl, activateClusterCmd(controllerClusterName, false), false);
    Thread.sleep(6000);
    Assert.assertFalse(_zkclient.exists("/" + controllerClusterName + "/IDEALSTATES/"
        + clusterName));

    HelixDataAccessor accessor = participants.get("localhost_1231").getHelixDataAccessor();
    String path = accessor.keyBuilder().controllerLeader().getPath();
    Assert.assertFalse(_zkclient.exists(path));

    deleteUrl(clusterUrl, true);
    Assert.assertTrue(_zkclient.exists("/" + clusterName));

    // leader node should be gone
    for (MockParticipant participant : participants.values()) {
      participant.syncStop();
    }
    deleteUrl(clusterUrl, false);

    Assert.assertFalse(_zkclient.exists("/" + clusterName));

    // clean up
    for (MockMultiClusterController controller : multiClusterControllers.values()) {
      controller.syncStop();
    }

    for (MockParticipant participant : participants.values()) {
      participant.syncStop();
    }
  }

  private Map<String, String> addIdealStateCmd() {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addIdealState);

    return parameters;
  }

  @Test
  public void testDropAddResource() throws Exception {
    final String clusterName = "clusterTestDropAddResource";

    // setup cluster
    addCluster(clusterName);
    addResource(clusterName, "db_11", 22);
    addInstancesToCluster(clusterName, "localhost_123", 6, null);
    rebalanceResource(clusterName, "db_11");
    ZNRecord record =
        _setupTool.getClusterManagementTool().getResourceIdealState(clusterName, "db_11")
            .getRecord();
    String x = ObjectToJson(record);

    FileWriter fos = new FileWriter("/tmp/temp.log");
    PrintWriter pw = new PrintWriter(fos);
    pw.write(x);
    pw.close();

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_9900");
    controller.syncStart();

    // start mock nodes
    Map<String, MockParticipant> participants =
        new HashMap<String, MockParticipant>();
    for (int i = 0; i < 6; i++) {
      String instanceName = "localhost_123" + i;
      MockParticipant participant =
          new MockParticipant(_zkaddr, clusterName, instanceName);
      participant.syncStart();
      participants.put(instanceName, participant);
    }
    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    String resourceUrl = getResourceUrl(clusterName, "db_11");
    deleteUrl(resourceUrl, false);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);
    addResource(clusterName, "db_11", 22);

    String idealStateUrl = getResourceUrl(clusterName, "db_11") + "/idealState";
    Map<String, String> extraform = new HashMap<String, String>();
    extraform.put(JsonParameters.NEW_IDEAL_STATE, x);
    assertSuccessPostOperation(idealStateUrl, addIdealStateCmd(), extraform, false);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    ZNRecord record2 =
        _setupTool.getClusterManagementTool().getResourceIdealState(clusterName, "db_11")
            .getRecord();
    Assert.assertTrue(record2.equals(record));

    // clean up
    controller.syncStop();
    for (MockParticipant participant : participants.values()) {
      participant.syncStop();
    }
  }

  private Map<String, String> addInstanceCmd(String instances) {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.INSTANCE_NAMES, instances);
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstance);

    return parameters;
  }

  private Map<String, String> expandClusterCmd() {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.expandCluster);

    return parameters;
  }

  @Test
  public void testExpandCluster() throws Exception {

    final String clusterName = "clusterTestExpandCluster";

    // setup cluster
    addCluster(clusterName);
    addInstancesToCluster(clusterName, "localhost:123", 6, null);
    addResource(clusterName, "db_11", 22);
    rebalanceResource(clusterName, "db_11");

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_9900");
    controller.syncStart();

    // start mock nodes
    Map<String, MockParticipant> participants =
        new HashMap<String, MockParticipant>();
    for (int i = 0; i < 6; i++) {
      String instanceName = "localhost_123" + i;
      MockParticipant participant =
          new MockParticipant(_zkaddr, clusterName, instanceName);
      participant.syncStart();
      participants.put(instanceName, participant);
    }

    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    String clusterUrl = getClusterUrl(clusterName);
    String instancesUrl = clusterUrl + "/instances";

    String instances = "localhost:12331;localhost:12341;localhost:12351;localhost:12361";
    String response = assertSuccessPostOperation(instancesUrl, addInstanceCmd(instances), false);
    String[] hosts = instances.split(";");
    for (String host : hosts) {
      Assert.assertTrue(response.contains(host.replace(':', '_')));
    }

    response = assertSuccessPostOperation(clusterUrl, expandClusterCmd(), false);

    for (int i = 3; i <= 6; i++) {
      String instanceName = "localhost_123" + i + "1";
      MockParticipant participant =
          new MockParticipant(_zkaddr, clusterName, instanceName);
      participant.syncStart();
      participants.put(instanceName, participant);
    }

    verifyResult =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(_zkaddr, clusterName));
    Assert.assertTrue(verifyResult);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    // clean up
    controller.syncStop();
    for (MockParticipant participant : participants.values()) {
      participant.syncStop();
    }
  }

  private Map<String, String> enablePartitionCmd(String resourceName, String partitions,
      boolean enabled) {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.enablePartition);
    parameters.put(JsonParameters.ENABLED, "" + enabled);
    parameters.put(JsonParameters.PARTITION, partitions);
    parameters.put(JsonParameters.RESOURCE, resourceName);

    return parameters;
  }

  @Test
  public void testEnablePartitions() throws IOException, InterruptedException {
    final String clusterName = "clusterTestEnablePartitions";

    // setup cluster
    addCluster(clusterName);
    addInstancesToCluster(clusterName, "localhost:123", 6, null);
    addResource(clusterName, "db_11", 22);
    rebalanceResource(clusterName, "db_11");

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_9900");
    controller.syncStart();

    // start mock nodes
    Map<String, MockParticipant> participants =
        new HashMap<String, MockParticipant>();
    for (int i = 0; i < 6; i++) {
      String instanceName = "localhost_123" + i;
      MockParticipant participant =
          new MockParticipant(_zkaddr, clusterName, instanceName);
      participant.syncStart();
      participants.put(instanceName, participant);
    }

    HelixDataAccessor accessor = participants.get("localhost_1231").getHelixDataAccessor();
    // drop node should fail as not disabled
    String hostName = "localhost_1231";
    String instanceUrl = getInstanceUrl(clusterName, hostName);
    ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView("db_11"));

    String response =
        assertSuccessPostOperation(instanceUrl,
            enablePartitionCmd("db_11", "db_11_0;db_11_11", false), false);
    Assert.assertTrue(response.contains("DISABLED_PARTITION"));
    Assert.assertTrue(response.contains("db_11_0"));
    Assert.assertTrue(response.contains("db_11_11"));

    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    ev = accessor.getProperty(accessor.keyBuilder().externalView("db_11"));
    Assert.assertEquals(ev.getStateMap("db_11_0").get(hostName), "OFFLINE");
    Assert.assertEquals(ev.getStateMap("db_11_11").get(hostName), "OFFLINE");

    response =
        assertSuccessPostOperation(instanceUrl,
            enablePartitionCmd("db_11", "db_11_0;db_11_11", true), false);
    Assert.assertFalse(response.contains("db_11_0"));
    Assert.assertFalse(response.contains("db_11_11"));

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    ev = accessor.getProperty(accessor.keyBuilder().externalView("db_11"));
    Assert.assertEquals(ev.getStateMap("db_11_0").get(hostName), "MASTER");
    Assert.assertEquals(ev.getStateMap("db_11_11").get(hostName), "SLAVE");

    // clean up
    controller.syncStop();
    for (MockParticipant participant : participants.values()) {
      participant.syncStop();
    }
  }

  private Map<String, String> enableInstanceCmd(boolean enabled) {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.enableInstance);
    parameters.put(JsonParameters.ENABLED, "" + enabled);
    return parameters;
  }

  private Map<String, String> swapInstanceCmd(String oldInstance, String newInstance) {
    Map<String, String> parameters = new HashMap<String, String>();

    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.swapInstance);
    parameters.put(JsonParameters.OLD_INSTANCE, oldInstance);
    parameters.put(JsonParameters.NEW_INSTANCE, newInstance);

    return parameters;
  }

  @Test
  public void testInstanceOperations() throws Exception {
    final String clusterName = "clusterTestInstanceOperations";

    // setup cluster
    addCluster(clusterName);
    addInstancesToCluster(clusterName, "localhost:123", 6, null);
    addResource(clusterName, "db_11", 8);
    rebalanceResource(clusterName, "db_11");

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_9900");
    controller.syncStart();

    // start mock nodes
    Map<String, MockParticipant> participants =
        new HashMap<String, MockParticipant>();
    for (int i = 0; i < 6; i++) {
      String instanceName = "localhost_123" + i;
      MockParticipant participant =
          new MockParticipant(_zkaddr, clusterName, instanceName);
      participant.syncStart();
      participants.put(instanceName, participant);
    }

    HelixDataAccessor accessor;
    // drop node should fail as not disabled
    String instanceUrl = getInstanceUrl(clusterName, "localhost_1232");
    deleteUrl(instanceUrl, true);

    // disabled node
    String response = assertSuccessPostOperation(instanceUrl, enableInstanceCmd(false), false);
    Assert.assertTrue(response.contains("false"));

    // Cannot drop / swap
    deleteUrl(instanceUrl, true);

    String instancesUrl = getClusterUrl(clusterName) + "/instances";
    response =
        assertSuccessPostOperation(instancesUrl,
            swapInstanceCmd("localhost_1232", "localhost_12320"), true);

    // disconnect the node
    participants.get("localhost_1232").syncStop();

    // add new node then swap instance
    response = assertSuccessPostOperation(instancesUrl, addInstanceCmd("localhost_12320"), false);
    Assert.assertTrue(response.contains("localhost_12320"));

    // swap instance. The instance get swapped out should not exist anymore
    response =
        assertSuccessPostOperation(instancesUrl,
            swapInstanceCmd("localhost_1232", "localhost_12320"), false);
    Assert.assertTrue(response.contains("localhost_12320"));
    Assert.assertFalse(response.contains("localhost_1232\""));

    accessor = participants.get("localhost_1231").getHelixDataAccessor();
    String path = accessor.keyBuilder().instanceConfig("localhost_1232").getPath();
    Assert.assertFalse(_zkclient.exists(path));

    MockParticipant newParticipant =
        new MockParticipant(_zkaddr, clusterName, "localhost_12320");
    newParticipant.syncStart();
    participants.put("localhost_12320", newParticipant);

    boolean verifyResult =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(_zkaddr, clusterName));
    Assert.assertTrue(verifyResult);

    // clean up
    controller.syncStop();
    for (MockParticipant participant : participants.values()) {
      participant.syncStop();
    }
  }

  @Test
  public void testStartCluster() throws Exception {
    final String clusterName = "clusterTestStartCluster";
    final String controllerClusterName = "controllerClusterTestStartCluster";

    Map<String, MockParticipant> participants =
        new HashMap<String, MockParticipant>();
    Map<String, MockMultiClusterController> multiClusterControllers =
        new HashMap<String, MockMultiClusterController>();

    // setup cluster
    addCluster(clusterName);
    addInstancesToCluster(clusterName, "localhost:123", 6, null);
    addResource(clusterName, "db_11", 8);
    rebalanceResource(clusterName, "db_11");

    addCluster(controllerClusterName);
    addInstancesToCluster(controllerClusterName, "controller_900", 2, null);

    // start mock nodes
    for (int i = 0; i < 6; i++) {
      String instanceName = "localhost_123" + i;
      MockParticipant participant =
          new MockParticipant(_zkaddr, clusterName, instanceName);
      participant.syncStart();
      participants.put(instanceName, participant);
    }

    // start controller nodes
    for (int i = 0; i < 2; i++) {
      String controllerName = "controller_900" + i;
      MockMultiClusterController multiClusterController =
          new MockMultiClusterController(_zkaddr, controllerClusterName, controllerName);
      multiClusterController.syncStart();
      multiClusterControllers.put(controllerName, multiClusterController);
    }
    Thread.sleep(100);

    // activate clusters
    // wrong grand clustername
    String clusterUrl = getClusterUrl(clusterName);
    assertSuccessPostOperation(clusterUrl, activateClusterCmd("nonExistCluster", true), true);

    // wrong cluster name
    clusterUrl = getClusterUrl("nonExistCluster");
    assertSuccessPostOperation(clusterUrl, activateClusterCmd(controllerClusterName, true), true);

    clusterUrl = getClusterUrl(clusterName);
    assertSuccessPostOperation(clusterUrl, activateClusterCmd(controllerClusterName, true), false);
    Thread.sleep(500);

    deleteUrl(clusterUrl, true);

    // verify leader node
    HelixDataAccessor accessor = multiClusterControllers.get("controller_9001").getHelixDataAccessor();
    LiveInstance controllerLeader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    Assert.assertTrue(controllerLeader.getInstanceName().startsWith("controller_900"));

    accessor = participants.get("localhost_1232").getHelixDataAccessor();
    LiveInstance leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    for (int i = 0; i < 5; i++) {
      if (leader != null) {
        break;
      }
      Thread.sleep(1000);
      leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    }
    Assert.assertTrue(leader.getInstanceName().startsWith("controller_900"));

    boolean verifyResult =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(_zkaddr, clusterName));
    Assert.assertTrue(verifyResult);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);
    Thread.sleep(1000);

    // clean up
    for (MockMultiClusterController controller : multiClusterControllers.values()) {
      controller.syncStop();
    }
    for (MockParticipant participant : participants.values()) {
      participant.syncStop();
    }
  }

  private Map<String, String> rebalanceCmd(int replicas, String prefix, String tag) {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.REPLICAS, "" + replicas);
    if (prefix != null) {
      parameters.put(JsonParameters.RESOURCE_KEY_PREFIX, prefix);
    }
    if (tag != null) {
      parameters.put(ClusterSetup.instanceGroupTag, tag);
    }
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.rebalance);

    return parameters;
  }

  private void rebalanceResource(String clusterName, String resourceName) throws IOException {
    String resourceUrl = getResourceUrl(clusterName, resourceName);
    String idealStateUrl = resourceUrl + "/idealState";

    assertSuccessPostOperation(idealStateUrl, rebalanceCmd(3, null, null), false);
  }

  @Test
  public void testRebalanceResource() throws Exception {
    // add a normal cluster
    final String clusterName = "clusterTestRebalanceResource";
    addCluster(clusterName);

    addInstancesToCluster(clusterName, "localhost:123", 3, _tag1);
    addResource(clusterName, "db_11", 44);

    String resourceUrl = getResourceUrl(clusterName, "db_11");

    String idealStateUrl = resourceUrl + "/idealState";
    String response = assertSuccessPostOperation(idealStateUrl, rebalanceCmd(3, null, null), false);
    ZNRecord record = JsonToObject(ZNRecord.class, response);
    Assert.assertTrue(record.getId().equalsIgnoreCase("db_11"));
    Assert.assertEquals(record.getListField("db_11_0").size(), 3);
    Assert.assertEquals(record.getMapField("db_11_0").size(), 3);

    deleteUrl(resourceUrl, false);

    // re-add and rebalance
    final String reourcesUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/resourceGroups";

    response = getUrl(reourcesUrl);
    Assert.assertFalse(response.contains("db_11"));

    addResource(clusterName, "db_11", 48);
    idealStateUrl = resourceUrl + "/idealState";
    response = assertSuccessPostOperation(idealStateUrl, rebalanceCmd(3, null, null), false);
    record = JsonToObject(ZNRecord.class, response);
    Assert.assertTrue(record.getId().equalsIgnoreCase("db_11"));
    Assert.assertEquals(record.getListField("db_11_0").size(), 3);
    Assert.assertEquals(record.getMapField("db_11_0").size(), 3);

    // rebalance with key prefix
    addResource(clusterName, "db_22", 55);
    resourceUrl = getResourceUrl(clusterName, "db_22");
    idealStateUrl = resourceUrl + "/idealState";
    response = assertSuccessPostOperation(idealStateUrl, rebalanceCmd(2, "alias", null), false);
    record = JsonToObject(ZNRecord.class, response);
    Assert.assertTrue(record.getId().equalsIgnoreCase("db_22"));
    Assert.assertEquals(record.getListField("alias_0").size(), 2);
    Assert.assertEquals(record.getMapField("alias_0").size(), 2);
    Assert.assertTrue((((String) (record.getMapFields().keySet().toArray()[0])))
        .startsWith("alias_"));
    Assert.assertFalse(response.contains(IdealStateProperty.INSTANCE_GROUP_TAG.toString()));

    addResource(clusterName, "db_33", 44);
    resourceUrl = getResourceUrl(clusterName, "db_33");
    idealStateUrl = resourceUrl + "/idealState";
    response = assertSuccessPostOperation(idealStateUrl, rebalanceCmd(2, null, _tag1), false);

    Assert.assertTrue(response.contains(IdealStateProperty.INSTANCE_GROUP_TAG.toString()));
    Assert.assertTrue(response.contains(_tag1));
    for (int i = 0; i < 6; i++) {
      String instance = "localhost_123" + i;
      if (i < 3) {
        Assert.assertTrue(response.contains(instance));
      } else {
        Assert.assertFalse(response.contains(instance));
      }
    }

    addResource(clusterName, "db_44", 44);
    resourceUrl = getResourceUrl(clusterName, "db_44");
    idealStateUrl = resourceUrl + "/idealState";
    response = assertSuccessPostOperation(idealStateUrl, rebalanceCmd(2, "alias", _tag1), false);
    Assert.assertTrue(response.contains(IdealStateProperty.INSTANCE_GROUP_TAG.toString()));
    Assert.assertTrue(response.contains(_tag1));

    record = JsonToObject(ZNRecord.class, response);
    Assert.assertTrue((((String) (record.getMapFields().keySet().toArray()[0])))
        .startsWith("alias_"));

    for (int i = 0; i < 6; i++) {
      String instance = "localhost_123" + i;
      if (i < 3) {
        Assert.assertTrue(response.contains(instance));
      } else {
        Assert.assertFalse(response.contains(instance));
      }
    }
  }

  private void addInstancesToCluster(String clusterName, String instanceNamePrefix, int n,
      String tag) throws IOException {
    Map<String, String> parameters = new HashMap<String, String>();
    final String clusterUrl = getClusterUrl(clusterName);
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstance);

    // add instances to cluster
    String instancesUrl = clusterUrl + "/instances";
    for (int i = 0; i < n; i++) {

      parameters.put(JsonParameters.INSTANCE_NAME, instanceNamePrefix + i);
      String response = assertSuccessPostOperation(instancesUrl, parameters, false);
      Assert.assertTrue(response.contains((instanceNamePrefix + i).replace(':', '_')));
    }

    // add tag to instance
    if (tag != null && !tag.isEmpty()) {
      parameters.clear();
      parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstanceTag);
      parameters.put(ClusterSetup.instanceGroupTag, tag);
      for (int i = 0; i < n; i++) {
        String instanceUrl = instancesUrl + "/" + (instanceNamePrefix + i).replace(':', '_');
        String response = assertSuccessPostOperation(instanceUrl, parameters, false);
        Assert.assertTrue(response.contains(_tag1));
      }
    }

  }

  private Map<String, String> addInstanceTagCmd(String tag) {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstanceTag);
    parameters.put(ClusterSetup.instanceGroupTag, tag);

    return parameters;
  }

  private Map<String, String> removeInstanceTagCmd(String tag) {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.removeInstanceTag);
    parameters.put(ClusterSetup.instanceGroupTag, tag);

    return parameters;
  }

  @Test
  public void testAddInstance() throws Exception {
    final String clusterName = "clusterTestAddInstance";

    // add normal cluster
    addCluster(clusterName);

    String clusterUrl = getClusterUrl(clusterName);

    // Add instances to cluster
    String instancesUrl = clusterUrl + "/instances";
    addInstancesToCluster(clusterName, "localhost:123", 3, null);

    String instances = "localhost:1233;localhost:1234;localhost:1235;localhost:1236";
    String response = assertSuccessPostOperation(instancesUrl, addInstanceCmd(instances), false);
    for (int i = 3; i <= 6; i++) {
      Assert.assertTrue(response.contains("localhost_123" + i));
    }

    // delete one node without disable
    String instanceUrl = instancesUrl + "/localhost_1236";
    deleteUrl(instanceUrl, true);
    response = getUrl(instancesUrl);
    Assert.assertTrue(response.contains("localhost_1236"));

    // delete non-exist node
    instanceUrl = instancesUrl + "/localhost_12367";
    deleteUrl(instanceUrl, true);
    response = getUrl(instancesUrl);
    Assert.assertFalse(response.contains("localhost_12367"));

    // disable node
    instanceUrl = instancesUrl + "/localhost_1236";
    response = assertSuccessPostOperation(instanceUrl, enableInstanceCmd(false), false);
    Assert.assertTrue(response.contains("false"));

    deleteUrl(instanceUrl, false);

    // add controller cluster
    final String controllerClusterName = "controllerClusterTestAddInstance";
    addCluster(controllerClusterName);

    // add node to controller cluster
    String controllers = "controller:9000;controller:9001";
    String controllerUrl = getClusterUrl(controllerClusterName) + "/instances";
    response = assertSuccessPostOperation(controllerUrl, addInstanceCmd(controllers), false);
    Assert.assertTrue(response.contains("controller_9000"));
    Assert.assertTrue(response.contains("controller_9001"));

    // add a duplicated host
    response = assertSuccessPostOperation(instancesUrl, addInstanceCmd("localhost:1234"), true);

    // add/remove tags
    for (int i = 0; i < 4; i++) {
      instanceUrl = instancesUrl + "/localhost_123" + i;
      response = assertSuccessPostOperation(instanceUrl, addInstanceTagCmd(_tag1), false);
      Assert.assertTrue(response.contains(_tag1));
    }

    instanceUrl = instancesUrl + "/localhost_1233";
    response = assertSuccessPostOperation(instanceUrl, removeInstanceTagCmd(_tag1), false);
    Assert.assertFalse(response.contains(_tag1));
  }

  @Test
  public void testGetResources() throws IOException {
    final String clusterName = "TestTagAwareness_testGetResources";
    final String TAG = "tag";
    final String URL_BASE =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/resourceGroups";

    _setupTool.addCluster(clusterName, true);
    HelixAdmin admin = _setupTool.getClusterManagementTool();

    // Add a tagged resource
    IdealState taggedResource = new IdealState("taggedResource");
    taggedResource.setInstanceGroupTag(TAG);
    taggedResource.setStateModelDefRef("OnlineOffline");
    admin.addResource(clusterName, taggedResource.getId(), taggedResource);

    // Add an untagged resource
    IdealState untaggedResource = new IdealState("untaggedResource");
    untaggedResource.setStateModelDefRef("OnlineOffline");
    admin.addResource(clusterName, untaggedResource.getId(), untaggedResource);

    // Now make a REST call for all resources
    Reference resourceRef = new Reference(URL_BASE);
    Request request = new Request(Method.GET, resourceRef);
    Response response = _gClient.handle(request);
    ZNRecord responseRecord =
        ClusterRepresentationUtil.JsonToObject(ZNRecord.class, response.getEntityAsText());

    // Ensure that the tagged resource has information and the untagged one doesn't
    Assert.assertNotNull(responseRecord.getMapField("ResourceTags"));
    Assert
        .assertEquals(TAG, responseRecord.getMapField("ResourceTags").get(taggedResource.getId()));
    Assert.assertFalse(responseRecord.getMapField("ResourceTags").containsKey(
        untaggedResource.getId()));
  }

  @Test
  public void testGetInstances() throws IOException {
    final String clusterName = "TestTagAwareness_testGetResources";
    final String[] TAGS = {
        "tag1", "tag2"
    };
    final String URL_BASE =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/instances";

    _setupTool.addCluster(clusterName, true);
    HelixAdmin admin = _setupTool.getClusterManagementTool();

    // Add 4 participants, each with differint tag characteristics
    InstanceConfig instance1 = new InstanceConfig("localhost_1");
    instance1.addTag(TAGS[0]);
    admin.addInstance(clusterName, instance1);
    InstanceConfig instance2 = new InstanceConfig("localhost_2");
    instance2.addTag(TAGS[1]);
    admin.addInstance(clusterName, instance2);
    InstanceConfig instance3 = new InstanceConfig("localhost_3");
    instance3.addTag(TAGS[0]);
    instance3.addTag(TAGS[1]);
    admin.addInstance(clusterName, instance3);
    InstanceConfig instance4 = new InstanceConfig("localhost_4");
    admin.addInstance(clusterName, instance4);

    // Now make a REST call for all resources
    Reference resourceRef = new Reference(URL_BASE);
    Request request = new Request(Method.GET, resourceRef);
    Response response = _gClient.handle(request);
    ListInstancesWrapper responseWrapper =
        ClusterRepresentationUtil.JsonToObject(ListInstancesWrapper.class,
            response.getEntityAsText());
    Map<String, List<String>> tagInfo = responseWrapper.tagInfo;

    // Ensure tag ownership is reported correctly
    Assert.assertTrue(tagInfo.containsKey(TAGS[0]));
    Assert.assertTrue(tagInfo.containsKey(TAGS[1]));
    Assert.assertTrue(tagInfo.get(TAGS[0]).contains("localhost_1"));
    Assert.assertFalse(tagInfo.get(TAGS[0]).contains("localhost_2"));
    Assert.assertTrue(tagInfo.get(TAGS[0]).contains("localhost_3"));
    Assert.assertFalse(tagInfo.get(TAGS[0]).contains("localhost_4"));
    Assert.assertFalse(tagInfo.get(TAGS[1]).contains("localhost_1"));
    Assert.assertTrue(tagInfo.get(TAGS[1]).contains("localhost_2"));
    Assert.assertTrue(tagInfo.get(TAGS[1]).contains("localhost_3"));
    Assert.assertFalse(tagInfo.get(TAGS[1]).contains("localhost_4"));
  }
}
