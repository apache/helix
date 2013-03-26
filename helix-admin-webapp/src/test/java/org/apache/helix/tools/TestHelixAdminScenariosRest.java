package org.apache.helix.tools;

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

/*
 * Simulate all the admin tasks needed by using command line tool
 * 
 * */
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.TestHelper.StartCMResult;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.apache.helix.webapp.RestAdminApplication;
import org.apache.helix.webapp.resources.ClusterRepresentationUtil;
import org.apache.helix.webapp.resources.JsonParameters;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.restlet.Client;
import org.restlet.Component;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelixAdminScenariosRest extends AdminTestBase
{
  Map<String, StartCMResult> _startCMResultMap = new HashMap<String, StartCMResult>();
  RestAdminApplication       _adminApp;
  Component                  _component;
  String _tag1 = "tag1123";
  String _tag2 = "tag212334";
  

  public static String ObjectToJson(Object object) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, object);

    return sw.toString();
  }

  public static <T extends Object> T JsonToObject(Class<T> clazz, String jsonString) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    StringReader sr = new StringReader(jsonString);
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(sr, clazz);
  }

  @Test
  public void testAddDeleteClusterAndInstanceAndResource() throws Exception
  {
    // Helix bug helix-102
    // ZKPropertyTransferServer.PERIOD = 500;
    // ZkPropertyTransferClient.SEND_PERIOD = 500;
    // ZKPropertyTransferServer.getInstance().init(19999, ZK_ADDR);

    /** ======================= Add clusters ============================== */

    testAddCluster();

    /** ================= Add / drop some resources =========================== */

    testAddResource();

    /** ====================== Add / delete instances =========================== */

    testAddInstance();

    /** ===================== Rebalance resource =========================== */

    testRebalanceResource();

    /** ==================== start the clusters ============================= */

    testStartCluster();

    /** ==================== drop add resource in live clusters =================== */
    testDropAddResource();

    /** ======================Operations with live node ============================ */

    testInstanceOperations();

    /** ======================Operations with partitions ============================ */

    testEnablePartitions();

    /** ============================ expand cluster =========================== */

    testExpandCluster();

    /** ============================ deactivate cluster =========================== */
    testDeactivateCluster();

    // wait all zk callbacks done
    Thread.sleep(1000);
  }

  static String assertSuccessPostOperation(String url,
                                           Map<String, String> jsonParameters,
                                           boolean hasException) throws IOException
  {
    Reference resourceRef = new Reference(url);

    Request request = new Request(Method.POST, resourceRef);
    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
                          + ClusterRepresentationUtil.ObjectToJson(jsonParameters),
                      MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    Assert.assertTrue(response.getStatus().getCode() == Status.SUCCESS_OK.getCode());
    Assert.assertTrue(hasException == sw.toString().toLowerCase().contains("exception"));
    return sw.toString();
  }

  static String assertSuccessPostOperation(String url,
                                           Map<String, String> jsonParameters,
                                           Map<String, String> extraForm,
                                           boolean hasException) throws IOException
  {
    Reference resourceRef = new Reference(url);

    Request request = new Request(Method.POST, resourceRef);
    String entity =
        JsonParameters.JSON_PARAMETERS + "="
            + ClusterRepresentationUtil.ObjectToJson(jsonParameters);
    for (String key : extraForm.keySet())
    {
      entity = entity + "&" + (key + "=" + extraForm.get(key));
    }
    request.setEntity(entity, MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    Assert.assertTrue(response.getStatus().getCode() == Status.SUCCESS_OK.getCode());
    Assert.assertTrue(hasException == sw.toString().toLowerCase().contains("exception"));
    return sw.toString();
  }

  void deleteUrl(String url, boolean hasException) throws IOException
  {
    Reference resourceRef = new Reference(url);
    Request request = new Request(Method.DELETE, resourceRef);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);
    Assert.assertTrue(hasException == sw.toString().toLowerCase().contains("exception"));
  }

  String getUrl(String url) throws IOException
  {
    Reference resourceRef = new Reference(url);
    Request request = new Request(Method.GET, resourceRef);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);
    return sw.toString();
  }

  String getClusterUrl(String cluster)
  {
    return "http://localhost:" + ADMIN_PORT + "/clusters" + "/" + cluster;
  }

  String getInstanceUrl(String cluster, String instance)
  {
    return "http://localhost:" + ADMIN_PORT + "/clusters/" + cluster + "/instances/"
        + instance;
  }

  String getResourceUrl(String cluster, String resourceGroup)
  {
    return "http://localhost:" + ADMIN_PORT + "/clusters/" + cluster + "/resourceGroups/"
        + resourceGroup;
  }

  void assertClusterSetupException(String command)
  {
    boolean exceptionThrown = false;
    try
    {
      ClusterSetup.processCommandLineArgs(command.split(" "));
    }
    catch (Exception e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  public void testAddCluster() throws Exception
  {
    String url = "http://localhost:" + ADMIN_PORT + "/clusters";
    Map<String, String> paraMap = new HashMap<String, String>();

    // Normal add
    paraMap.put(JsonParameters.CLUSTER_NAME, "clusterTest");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addCluster);

    String response = assertSuccessPostOperation(url, paraMap, false);
    Assert.assertTrue(response.contains("clusterTest"));

    // malformed cluster name
    paraMap.put(JsonParameters.CLUSTER_NAME, "/ClusterTest");
    response = assertSuccessPostOperation(url, paraMap, true);

    // Add the grand cluster
    paraMap.put(JsonParameters.CLUSTER_NAME, "Klazt3rz");
    response = assertSuccessPostOperation(url, paraMap, false);
    Assert.assertTrue(response.contains("Klazt3rz"));

    paraMap.put(JsonParameters.CLUSTER_NAME, "\\ClusterTest");
    response = assertSuccessPostOperation(url, paraMap, false);
    Assert.assertTrue(response.contains("\\ClusterTest"));

    // Add already exist cluster
    paraMap.put(JsonParameters.CLUSTER_NAME, "clusterTest");
    response = assertSuccessPostOperation(url, paraMap, true);

    // delete cluster without resource and instance
    Assert.assertTrue(ZKUtil.isClusterSetup("Klazt3rz", _gZkClient));
    Assert.assertTrue(ZKUtil.isClusterSetup("clusterTest", _gZkClient));
    Assert.assertTrue(ZKUtil.isClusterSetup("\\ClusterTest", _gZkClient));

    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.dropCluster);

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

    Assert.assertFalse(_gZkClient.exists("/clusterTest"));
    Assert.assertFalse(_gZkClient.exists("/clusterTest1"));
    Assert.assertFalse(_gZkClient.exists("/clusterTestOK"));

    paraMap.put(JsonParameters.CLUSTER_NAME, "clusterTest1");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addCluster);
    response = assertSuccessPostOperation(url, paraMap, false);
    response = getUrl(clustersUrl);
    Assert.assertTrue(response.contains("clusterTest1"));
  }

  public void testAddResource() throws Exception
  {
    String reourcesUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/clusterTest1/resourceGroups";

    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.RESOURCE_GROUP_NAME, "db_22");
    paraMap.put(JsonParameters.STATE_MODEL_DEF_REF, "MasterSlave");
    paraMap.put(JsonParameters.PARTITIONS, "144");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addResource);

    String response = assertSuccessPostOperation(reourcesUrl, paraMap, false);
    Assert.assertTrue(response.contains("db_22"));

    paraMap.put(JsonParameters.RESOURCE_GROUP_NAME, "db_11");
    paraMap.put(JsonParameters.STATE_MODEL_DEF_REF, "MasterSlave");
    paraMap.put(JsonParameters.PARTITIONS, "44");

    response = assertSuccessPostOperation(reourcesUrl, paraMap, false);
    Assert.assertTrue(response.contains("db_11"));

    // Add duplicate resource
    paraMap.put(JsonParameters.RESOURCE_GROUP_NAME, "db_22");
    paraMap.put(JsonParameters.STATE_MODEL_DEF_REF, "OnlineOffline");
    paraMap.put(JsonParameters.PARTITIONS, "55");

    response = assertSuccessPostOperation(reourcesUrl, paraMap, true);

    // drop resource now
    String resourceUrl = getResourceUrl("clusterTest1", "db_11");
    deleteUrl(resourceUrl, false);
    Assert.assertFalse(_gZkClient.exists("/clusterTest1/IDEALSTATES/db_11"));

    paraMap.put(JsonParameters.RESOURCE_GROUP_NAME, "db_11");
    paraMap.put(JsonParameters.STATE_MODEL_DEF_REF, "MasterSlave");
    paraMap.put(JsonParameters.PARTITIONS, "44");
    response = assertSuccessPostOperation(reourcesUrl, paraMap, false);
    Assert.assertTrue(response.contains("db_11"));

    Assert.assertTrue(_gZkClient.exists("/clusterTest1/IDEALSTATES/db_11"));
    
    paraMap.put(JsonParameters.RESOURCE_GROUP_NAME, "db_33");
    response = assertSuccessPostOperation(reourcesUrl, paraMap, false);
    Assert.assertTrue(response.contains("db_33"));
    
    paraMap.put(JsonParameters.RESOURCE_GROUP_NAME, "db_44");
    response = assertSuccessPostOperation(reourcesUrl, paraMap, false);
    Assert.assertTrue(response.contains("db_44"));
  }

  private void testDeactivateCluster() throws Exception,
      InterruptedException
  {
    HelixDataAccessor accessor;
    String path;
    // deactivate cluster
    String clusterUrl = getClusterUrl("clusterTest1");
    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.ENABLED, "false");
    paraMap.put(JsonParameters.GRAND_CLUSTER, "Klazt3rz");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.activateCluster);

    String response = assertSuccessPostOperation(clusterUrl, paraMap, false);
    Thread.sleep(6000);
    Assert.assertFalse(_gZkClient.exists("/Klazt3rz/IDEALSTATES/clusterTest1"));

    accessor = _startCMResultMap.get("localhost_1231")._manager.getHelixDataAccessor();
    path = accessor.keyBuilder().controllerLeader().getPath();
    Assert.assertFalse(_gZkClient.exists(path));

    deleteUrl(clusterUrl, true);

    Assert.assertTrue(_gZkClient.exists("/clusterTest1"));
    // leader node should be gone
    for (StartCMResult result : _startCMResultMap.values())
    {
      result._manager.disconnect();
      result._thread.interrupt();
    }
    deleteUrl(clusterUrl, false);

    Assert.assertFalse(_gZkClient.exists("/clusterTest1"));
  }

  private void testDropAddResource() throws Exception
  {
    ZNRecord record =
        _gSetupTool._admin.getResourceIdealState("clusterTest1", "db_11").getRecord();
    String x = ObjectToJson(record);

    FileWriter fos = new FileWriter("/tmp/temp.log");
    PrintWriter pw = new PrintWriter(fos);
    pw.write(x);
    pw.close();

    String resourceUrl = getResourceUrl("clusterTest1", "db_11");
    deleteUrl(resourceUrl, false);

    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);
    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addResource);
    paraMap.put(JsonParameters.RESOURCE_GROUP_NAME, "db_11");
    paraMap.put(JsonParameters.PARTITIONS, "22");
    paraMap.put(JsonParameters.STATE_MODEL_DEF_REF, "MasterSlave");
    String response =
        assertSuccessPostOperation(getClusterUrl("clusterTest1") + "/resourceGroups",
                                   paraMap,
                                   false);

    String idealStateUrl = getResourceUrl("clusterTest1", "db_11") + "/idealState";
    Assert.assertTrue(response.contains("db_11"));
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addIdealState);
    Map<String, String> extraform = new HashMap<String, String>();
    extraform.put(JsonParameters.NEW_IDEAL_STATE, x);
    response = assertSuccessPostOperation(idealStateUrl, paraMap, extraform, false);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);

    ZNRecord record2 =
        _gSetupTool._admin.getResourceIdealState("clusterTest1", "db_11").getRecord();
    Assert.assertTrue(record2.equals(record));
  }

  private void testExpandCluster() throws Exception
  {
    boolean verifyResult;

    String clusterUrl = getClusterUrl("clusterTest1");
    String instancesUrl = clusterUrl + "/instances";

    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.INSTANCE_NAMES,
                "localhost:12331;localhost:12341;localhost:12351;localhost:12361");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstance);

    String response = assertSuccessPostOperation(instancesUrl, paraMap, false);
    String[] hosts =
        "localhost:12331;localhost:12341;localhost:12351;localhost:12361".split(";");
    for (String host : hosts)
    {
      Assert.assertTrue(response.contains(host.replace(':', '_')));
    }
    paraMap.clear();
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.expandCluster);
    response = assertSuccessPostOperation(clusterUrl, paraMap, false);

    for (int i = 3; i <= 6; i++)
    {
      StartCMResult result =
          TestHelper.startDummyProcess(ZK_ADDR, "clusterTest1", "localhost_123" + i + "1");
      _startCMResultMap.put("localhost_123" + i + "1", result);
    }

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              "clusterTest1"));
    Assert.assertTrue(verifyResult);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);
  }

  private void testEnablePartitions() throws IOException,
      InterruptedException
  {
    HelixDataAccessor accessor;
    accessor = _startCMResultMap.get("localhost_1231")._manager.getHelixDataAccessor();
    // drop node should fail as not disabled
    String hostName = "localhost_1231";
    String instanceUrl = getInstanceUrl("clusterTest1", hostName);
    ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView("db_11"));

    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.enablePartition);
    paraMap.put(JsonParameters.ENABLED, "false");
    paraMap.put(JsonParameters.PARTITION, "db_11_0;db_11_15");
    paraMap.put(JsonParameters.RESOURCE, "db_11");

    String response = assertSuccessPostOperation(instanceUrl, paraMap, false);
    Assert.assertTrue(response.contains("DISABLED_PARTITION"));
    Assert.assertTrue(response.contains("db_11_0"));
    Assert.assertTrue(response.contains("db_11_15"));

    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);

    ev = accessor.getProperty(accessor.keyBuilder().externalView("db_11"));
    Assert.assertEquals(ev.getStateMap("db_11_0").get(hostName), "OFFLINE");
    Assert.assertEquals(ev.getStateMap("db_11_15").get(hostName), "OFFLINE");

    paraMap.put(JsonParameters.ENABLED, "true");
    response = assertSuccessPostOperation(instanceUrl, paraMap, false);
    Assert.assertFalse(response.contains("db_11_0"));
    Assert.assertFalse(response.contains("db_11_15"));

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);

    ev = accessor.getProperty(accessor.keyBuilder().externalView("db_11"));
    Assert.assertEquals(ev.getStateMap("db_11_0").get(hostName), "MASTER");
    Assert.assertEquals(ev.getStateMap("db_11_15").get(hostName), "SLAVE");
  }

  private void testInstanceOperations() throws Exception
  {
    HelixDataAccessor accessor;
    // drop node should fail as not disabled
    String instanceUrl = getInstanceUrl("clusterTest1", "localhost_1232");
    deleteUrl(instanceUrl, true);

    // disabled node
    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.enableInstance);
    paraMap.put(JsonParameters.ENABLED, "false");
    String response = assertSuccessPostOperation(instanceUrl, paraMap, false);
    Assert.assertTrue(response.contains("false"));

    // Cannot drop / swap
    deleteUrl(instanceUrl, true);

    String instancesUrl = getClusterUrl("clusterTest1") + "/instances";
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.swapInstance);
    paraMap.put(JsonParameters.OLD_INSTANCE, "localhost_1232");
    paraMap.put(JsonParameters.NEW_INSTANCE, "localhost_12320");
    response = assertSuccessPostOperation(instancesUrl, paraMap, true);

    // disconnect the node
    _startCMResultMap.get("localhost_1232")._manager.disconnect();
    _startCMResultMap.get("localhost_1232")._thread.interrupt();

    // add new node then swap instance
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstance);
    paraMap.put(JsonParameters.INSTANCE_NAME, "localhost_12320");
    response = assertSuccessPostOperation(instancesUrl, paraMap, false);
    Assert.assertTrue(response.contains("localhost_12320"));

    // swap instance. The instance get swapped out should not exist anymore
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.swapInstance);
    paraMap.put(JsonParameters.OLD_INSTANCE, "localhost_1232");
    paraMap.put(JsonParameters.NEW_INSTANCE, "localhost_12320");
    response = assertSuccessPostOperation(instancesUrl, paraMap, false);
    Assert.assertTrue(response.contains("localhost_12320"));
    Assert.assertFalse(response.contains("localhost_1232\""));

    accessor = _startCMResultMap.get("localhost_1231")._manager.getHelixDataAccessor();
    String path = accessor.keyBuilder().instanceConfig("localhost_1232").getPath();
    Assert.assertFalse(_gZkClient.exists(path));

    _startCMResultMap.put("localhost_12320",
                          TestHelper.startDummyProcess(ZK_ADDR,
                                                       "clusterTest1",
                                                       "localhost_12320"));
  }

  private void testStartCluster() throws Exception,
      InterruptedException
  {
    // start mock nodes
    for (int i = 0; i < 6; i++)
    {
      StartCMResult result =
          TestHelper.startDummyProcess(ZK_ADDR, "clusterTest1", "localhost_123" + i);
      _startCMResultMap.put("localhost_123" + i, result);
    }

    // start controller nodes
    for (int i = 0; i < 2; i++)
    {
      StartCMResult result =
          TestHelper.startController("Klazt3rz",
                                     "controller_900" + i,
                                     ZK_ADDR,
                                     HelixControllerMain.DISTRIBUTED);

      _startCMResultMap.put("controller_900" + i, result);
    }
    Thread.sleep(100);

    // activate clusters
    // wrong grand clustername

    String clusterUrl = getClusterUrl("clusterTest1");
    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.ENABLED, "true");
    paraMap.put(JsonParameters.GRAND_CLUSTER, "Klazters");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.activateCluster);

    String response = assertSuccessPostOperation(clusterUrl, paraMap, true);

    // wrong cluster name
    clusterUrl = getClusterUrl("clusterTest2");
    paraMap.put(JsonParameters.GRAND_CLUSTER, "Klazt3rz");
    response = assertSuccessPostOperation(clusterUrl, paraMap, true);

    paraMap.put(JsonParameters.ENABLED, "true");
    paraMap.put(JsonParameters.GRAND_CLUSTER, "Klazt3rz");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.activateCluster);
    clusterUrl = getClusterUrl("clusterTest1");
    response = assertSuccessPostOperation(clusterUrl, paraMap, false);
    Thread.sleep(500);

    deleteUrl(clusterUrl, true);

    // verify leader node
    HelixDataAccessor accessor =
        _startCMResultMap.get("controller_9001")._manager.getHelixDataAccessor();
    LiveInstance controllerLeader =
        accessor.getProperty(accessor.keyBuilder().controllerLeader());
    Assert.assertTrue(controllerLeader.getInstanceName().startsWith("controller_900"));

    accessor = _startCMResultMap.get("localhost_1232")._manager.getHelixDataAccessor();
    LiveInstance leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    for(int i = 0; i < 5; i++)
    {
      if(leader != null)
      {
        break;
      }
      Thread.sleep(1000);
      leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    }
    Assert.assertTrue(leader.getInstanceName().startsWith("controller_900"));

    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              "clusterTest1"));
    Assert.assertTrue(verifyResult);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);
  }

  private void testRebalanceResource() throws Exception
  {
    String resourceUrl = getResourceUrl("clusterTest1", "db_11");
    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.REPLICAS, "3");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.rebalance);

    String ISUrl = resourceUrl + "/idealState";
    String response = assertSuccessPostOperation(ISUrl, paraMap, false);
    ZNRecord record = JsonToObject(ZNRecord.class, response);
    Assert.assertTrue(record.getId().equalsIgnoreCase("db_11"));
    Assert.assertTrue((((List<String>) (record.getListFields().values().toArray()[0]))).size() == 3);
    Assert.assertTrue((((Map<String, String>) (record.getMapFields().values().toArray()[0]))).size() == 3);

    deleteUrl(resourceUrl, false);

    // re-add and rebalance
    String reourcesUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/clusterTest1/resourceGroups";
    response = getUrl(reourcesUrl);
    Assert.assertFalse(response.contains("db_11"));

    paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.RESOURCE_GROUP_NAME, "db_11");
    paraMap.put(JsonParameters.STATE_MODEL_DEF_REF, "MasterSlave");
    paraMap.put(JsonParameters.PARTITIONS, "48");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addResource);

    response = assertSuccessPostOperation(reourcesUrl, paraMap, false);
    Assert.assertTrue(response.contains("db_11"));

    ISUrl = resourceUrl + "/idealState";
    paraMap.put(JsonParameters.REPLICAS, "3");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.rebalance);
    response = assertSuccessPostOperation(ISUrl, paraMap, false);
    record = JsonToObject(ZNRecord.class, response);
    Assert.assertTrue(record.getId().equalsIgnoreCase("db_11"));
    Assert.assertTrue((((List<String>) (record.getListFields().values().toArray()[0]))).size() == 3);
    Assert.assertTrue((((Map<String, String>) (record.getMapFields().values().toArray()[0]))).size() == 3);

    // rebalance with key prefix
    resourceUrl = getResourceUrl("clusterTest1", "db_22");
    ISUrl = resourceUrl + "/idealState";
    paraMap.put(JsonParameters.REPLICAS, "2");
    paraMap.put(JsonParameters.RESOURCE_KEY_PREFIX, "alias");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.rebalance);
    response = assertSuccessPostOperation(ISUrl, paraMap, false);
    record = JsonToObject(ZNRecord.class, response);
    Assert.assertTrue(record.getId().equalsIgnoreCase("db_22"));
    Assert.assertTrue((((List<String>) (record.getListFields().values().toArray()[0]))).size() == 2);
    Assert.assertTrue((((Map<String, String>) (record.getMapFields().values().toArray()[0]))).size() == 2);
    Assert.assertTrue((((String) (record.getMapFields().keySet().toArray()[0]))).startsWith("alias_"));
    Assert.assertFalse(response.contains(IdealStateProperty.INSTANCE_GROUP_TAG.toString()));
    resourceUrl = getResourceUrl("clusterTest1", "db_33");
    ISUrl = resourceUrl + "/idealState";
    paraMap.put(JsonParameters.REPLICAS, "2");
    paraMap.remove(JsonParameters.RESOURCE_KEY_PREFIX);
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.rebalance);
    paraMap.put(ClusterSetup.instanceGroupTag,_tag1);
    response = assertSuccessPostOperation(ISUrl, paraMap, false);

    Assert.assertTrue(response.contains(IdealStateProperty.INSTANCE_GROUP_TAG.toString()));
    Assert.assertTrue(response.contains(_tag1));
    for (int i = 0; i < 6; i++)
    {
      String instance = "localhost_123"+i;
      if(i<3)
      {
        Assert.assertTrue(response.contains(instance));
      }
      else
      {
        Assert.assertFalse(response.contains(instance));
      }
    }
    
    resourceUrl = getResourceUrl("clusterTest1", "db_44");
    ISUrl = resourceUrl + "/idealState";
    paraMap.put(JsonParameters.REPLICAS, "2");
    paraMap.remove(JsonParameters.RESOURCE_KEY_PREFIX);
    paraMap.put(JsonParameters.RESOURCE_KEY_PREFIX, "alias");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.rebalance);
    paraMap.put(ClusterSetup.instanceGroupTag,_tag1);
    response = assertSuccessPostOperation(ISUrl, paraMap, false);
    Assert.assertTrue(response.contains(IdealStateProperty.INSTANCE_GROUP_TAG.toString()));
    Assert.assertTrue(response.contains(_tag1));

    record = JsonToObject(ZNRecord.class, response);
    Assert.assertTrue((((String) (record.getMapFields().keySet().toArray()[0]))).startsWith("alias_"));
    
    for (int i = 0; i < 6; i++)
    {
      String instance = "localhost_123"+i;
      if(i<3)
      {
        Assert.assertTrue(response.contains(instance));
      }
      else
      {
        Assert.assertFalse(response.contains(instance));
      }
    }
  }

  private void testAddInstance() throws Exception
  {
    String clusterUrl = getClusterUrl("clusterTest1");
    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstance);
    String response = null;
    // Add instances to cluster
    String instancesUrl = clusterUrl + "/instances";
    for (int i = 0; i < 3; i++)
    {

      paraMap.put(JsonParameters.INSTANCE_NAME, "localhost:123" + i);
      response = assertSuccessPostOperation(instancesUrl, paraMap, false);
      Assert.assertTrue(response.contains(("localhost:123" + i).replace(':', '_')));
    }
    paraMap.remove(JsonParameters.INSTANCE_NAME);
    paraMap.put(JsonParameters.INSTANCE_NAMES,
                "localhost:1233;localhost:1234;localhost:1235;localhost:1236");

    response = assertSuccessPostOperation(instancesUrl, paraMap, false);
    for (int i = 3; i <= 6; i++)
    {
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
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.enableInstance);
    paraMap.put(JsonParameters.ENABLED, "false");
    response = assertSuccessPostOperation(instanceUrl, paraMap, false);
    Assert.assertTrue(response.contains("false"));

    deleteUrl(instanceUrl, false);

    // add node to controller cluster
    paraMap.remove(JsonParameters.INSTANCE_NAME);
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstance);
    paraMap.put(JsonParameters.INSTANCE_NAMES, "controller:9000;controller:9001");
    String controllerUrl = getClusterUrl("Klazt3rz") + "/instances";
    response = assertSuccessPostOperation(controllerUrl, paraMap, false);
    Assert.assertTrue(response.contains("controller_9000"));
    Assert.assertTrue(response.contains("controller_9001"));

    // add a dup host
    paraMap.remove(JsonParameters.INSTANCE_NAMES);
    paraMap.put(JsonParameters.INSTANCE_NAME, "localhost:1234");
    response = assertSuccessPostOperation(instancesUrl, paraMap, true);
    
    // add tags
    
    paraMap.clear();
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstanceTag);
    paraMap.put(ClusterSetup.instanceGroupTag, _tag1);
    for (int i = 0; i < 4; i++)
    {
      instanceUrl = instancesUrl + "/localhost_123"+i;
      response = assertSuccessPostOperation(instanceUrl, paraMap, false);
      Assert.assertTrue(response.contains(_tag1));
      
    }
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.removeInstanceTag);
    instanceUrl = instancesUrl + "/localhost_1233";
    response = assertSuccessPostOperation(instanceUrl, paraMap, false);
    Assert.assertFalse(response.contains(_tag1));
    
  }
}
