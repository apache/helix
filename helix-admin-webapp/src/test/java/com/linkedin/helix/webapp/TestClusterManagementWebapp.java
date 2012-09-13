/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.webapp;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.restlet.Client;
import org.restlet.Component;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.InstanceConfig.InstanceConfigProperty;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.resources.ClusterRepresentationUtil;
import com.linkedin.helix.webapp.resources.JsonParameters;

public class TestClusterManagementWebapp
{
  private String         _zkServerAddress;
  private List<ZkServer> _localZkServers;
  private ZkClient       _zkclient;

  RestAdminApplication   _adminApp;
  Component              _component;

  int                    _port = 2200;

  public static List<ZkServer> startLocalZookeeper(List<Integer> localPortsList,
                                                   String zkTestDataRootDir,
                                                   int tickTime) throws IOException
  {
    List<ZkServer> localZkServers = new ArrayList<ZkServer>();

    int count = 0;
    for (int port : localPortsList)
    {
      ZkServer zkServer = startZkServer(zkTestDataRootDir, count++, port, tickTime);
      localZkServers.add(zkServer);
    }
    return localZkServers;
  }

  public static ZkServer startZkServer(String zkTestDataRootDir,
                                       int machineId,
                                       int port,
                                       int tickTime) throws IOException
  {
    File zkTestDataRootDirFile = new File(zkTestDataRootDir);
    zkTestDataRootDirFile.mkdirs();

    String dataPath = zkTestDataRootDir + "/" + machineId + "/" + port + "/data";
    String logPath = zkTestDataRootDir + "/" + machineId + "/" + port + "/log";

    FileUtils.deleteDirectory(new File(dataPath));
    FileUtils.deleteDirectory(new File(logPath));

    IDefaultNameSpace mockDefaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
      {
      }
    };

    ZkServer zkServer =
        new ZkServer(dataPath, logPath, mockDefaultNameSpace, port, tickTime);
    zkServer.start();
    return zkServer;
  }

  public static void stopLocalZookeeper(List<ZkServer> localZkServers)
  {
    for (ZkServer zkServer : localZkServers)
    {
      zkServer.shutdown();
    }
  }

  void startAdminWebAppThread() throws Exception
  {
    Thread t = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        HelixAdminWebApp app = null;
        try
        {
          app = new HelixAdminWebApp(_zkServerAddress, _port);
          app.start();
          Thread.currentThread().join();
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
        finally
        {
          if (app != null)
          {
            app.stop();
          }
        }
      }
    });
    t.setDaemon(true);
    t.start();
  }

  @BeforeTest
  public void setup() throws IOException,
      Exception
  {
    // TODO: use logging.properties file to config java.util.logging.Logger levels
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);

    List<Integer> localPorts = new ArrayList<Integer>();
    localPorts.add(2199);

    _localZkServers =
        startLocalZookeeper(localPorts,
                            System.getProperty("user.dir") + "/" + "zkdata",
                            2000);
    _zkServerAddress = "localhost:" + 2199;
    _zkclient =
        new ZkClient(_zkServerAddress,
                     ZkClient.DEFAULT_SESSION_TIMEOUT,
                     ZkClient.DEFAULT_CONNECTION_TIMEOUT,
                     new ZNRecordSerializer());

    System.out.println("Started zookeeper on " + _zkServerAddress);
    startAdminWebAppThread();
    System.out.println("Started Helix WebApp on " + _port);
  }

  // @AfterMethod
  @AfterTest
  public void tearDown() throws Exception
  {
    if (_zkclient != null)
    {
      _zkclient.close();
    }
    stopLocalZookeeper(_localZkServers);
    System.out.println("zk stopped!!");
  }

  @Test
  public void testInvocation() throws Exception
  {
    verifyAddCluster();
    verifyAddStateModel();
    verifyAddHostedEntity();
    verifyAddInstance();
    verifyRebalance();
    verifyEnableInstance();
    verifyAlterIdealState();
    verifyConfigAccessor();

    verifyEnableCluster();

    System.out.println("Test passed!!");
  }

  /*
   * Test case as steps
   */
  String clusterName       = "cluster-12345";
  String resourceGroupName = "new-entity-12345";
  String instance1         = "test-1";
  String statemodel        = "state_model";
  int    instancePort      = 9999;
  int    partitions        = 10;
  int    replicas          = 3;

  void verifyAddStateModel() throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    String httpUrlBase =
        "http://localhost:" + _port + "/clusters/" + clusterName
            + "/StateModelDefs/MasterSlave";
    Reference resourceRef = new Reference(httpUrlBase);
    Request request = new Request(Method.GET, resourceRef);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);
    ObjectMapper mapper = new ObjectMapper();
    ZNRecord zn = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);

    Map<String, String> paraMap = new HashMap<String, String>();

    paraMap.put(JsonParameters.MANAGEMENT_COMMAND,
                ClusterSetup.addStateModelDef);

    ZNRecord r = new ZNRecord("Test");
    r.merge(zn);
    StateModelDefinition newStateModel = new StateModelDefinition(r);

    httpUrlBase =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/StateModelDefs";
    resourceRef = new Reference(httpUrlBase);
    request = new Request(Method.POST, resourceRef);
    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
                          + ClusterRepresentationUtil.ObjectToJson(paraMap) + "&"
                          + JsonParameters.NEW_STATE_MODEL_DEF + "="
                          + ClusterRepresentationUtil.ZNRecordToJson(r),
                      MediaType.APPLICATION_ALL);
    client = new Client(Protocol.HTTP);
    response = client.handle(request);

    result = response.getEntity();
    sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    AssertJUnit.assertTrue(sw.toString().contains("Test"));
  }

  void verifyAddCluster() throws IOException,
      InterruptedException
  {
    String httpUrlBase = "http://localhost:" + _port + "/clusters/";
    Map<String, String> paraMap = new HashMap<String, String>();

    paraMap.put(JsonParameters.CLUSTER_NAME, clusterName);
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addCluster);

    Reference resourceRef = new Reference(httpUrlBase);

    Request request = new Request(Method.POST, resourceRef);

    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(paraMap), MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);

    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    ObjectMapper mapper = new ObjectMapper();
    ZNRecord zn = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);
    AssertJUnit.assertTrue(zn.getListField("clusters").contains(clusterName));

  }

  void verifyAddHostedEntity() throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    String httpUrlBase =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/resourceGroups";
    Map<String, String> paraMap = new HashMap<String, String>();

    paraMap.put(JsonParameters.RESOURCE_GROUP_NAME, resourceGroupName);
    paraMap.put(JsonParameters.PARTITIONS, "" + partitions);
    paraMap.put(JsonParameters.STATE_MODEL_DEF_REF, "MasterSlave");
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addResource);

    Reference resourceRef = new Reference(httpUrlBase);

    Request request = new Request(Method.POST, resourceRef);

    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(paraMap), MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);

    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    ObjectMapper mapper = new ObjectMapper();
    ZNRecord zn = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);
    AssertJUnit.assertTrue(zn.getListField("ResourceGroups").contains(resourceGroupName));

    httpUrlBase =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/resourceGroups/"
            + resourceGroupName;
    resourceRef = new Reference(httpUrlBase);

    request = new Request(Method.GET, resourceRef);

    client = new Client(Protocol.HTTP);
    response = client.handle(request);

    result = response.getEntity();
    sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());
  }

  void verifyAddInstance() throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    String httpUrlBase =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/instances";
    Map<String, String> paraMap = new HashMap<String, String>();
    // Add 1 instance
    paraMap.put(JsonParameters.INSTANCE_NAME, instance1 + ":" + instancePort);
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstance);

    Reference resourceRef = new Reference(httpUrlBase);

    Request request = new Request(Method.POST, resourceRef);

    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(paraMap), MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);

    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    ObjectMapper mapper = new ObjectMapper();

    TypeReference<ArrayList<ZNRecord>> typeRef = new TypeReference<ArrayList<ZNRecord>>()
    {
    };
    List<ZNRecord> znList = mapper.readValue(new StringReader(sw.toString()), typeRef);
    AssertJUnit.assertTrue(znList.get(0).getId().equals(instance1 + "_" + instancePort));

    // the case to add more than 1 instances
    paraMap.clear();
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addInstance);

    String[] instances = { "test2", "test3", "test4", "test5" };

    String instanceNames = "";
    boolean first = true;
    for (String instance : instances)
    {
      if (first == true)
      {
        first = false;
      }
      else
      {
        instanceNames += ";";
      }
      instanceNames += (instance + ":" + instancePort);
    }
    paraMap.put(JsonParameters.INSTANCE_NAMES, instanceNames);

    request = new Request(Method.POST, resourceRef);

    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(paraMap), MediaType.APPLICATION_ALL);
    client = new Client(Protocol.HTTP);
    response = client.handle(request);

    result = response.getEntity();
    sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    mapper = new ObjectMapper();

    znList = mapper.readValue(new StringReader(sw.toString()), typeRef);

    for (String instance : instances)
    {
      boolean found = false;
      for (ZNRecord r : znList)
      {
        String instanceId = instance + "_" + instancePort;
        if (r.getId().equals(instanceId))
        {
          found = true;
          break;
        }
      }
      AssertJUnit.assertTrue(found);
    }
  }

  void verifyRebalance() throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    String httpUrlBase =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/resourceGroups/"
            + resourceGroupName + "/idealState";
    Map<String, String> paraMap = new HashMap<String, String>();
    // Add 1 instance
    paraMap.put(JsonParameters.REPLICAS, "" + replicas);
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.rebalance);

    Reference resourceRef = new Reference(httpUrlBase);

    Request request = new Request(Method.POST, resourceRef);

    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(paraMap), MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);

    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    ObjectMapper mapper = new ObjectMapper();
    ZNRecord r = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);

    for (int i = 0; i < partitions; i++)
    {
      String partitionName = resourceGroupName + "_" + i;
      assert (r.getMapField(partitionName).size() == replicas);
    }

    httpUrlBase = "http://localhost:" + _port + "/clusters/" + clusterName;
    resourceRef = new Reference(httpUrlBase);
    request = new Request(Method.GET, resourceRef);

    client = new Client(Protocol.HTTP);
    response = client.handle(request);

    result = response.getEntity();
    sw = new StringWriter();
    result.write(sw);

  }

  void verifyEnableInstance() throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    String httpUrlBase =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/instances/"
            + instance1 + "_" + instancePort;
    Map<String, String> paraMap = new HashMap<String, String>();
    // Add 1 instance
    paraMap.put(JsonParameters.ENABLED, "" + false);
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.enableInstance);

    Reference resourceRef = new Reference(httpUrlBase);

    Request request = new Request(Method.POST, resourceRef);

    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(paraMap), MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);

    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    ObjectMapper mapper = new ObjectMapper();
    ZNRecord r = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);
    AssertJUnit.assertTrue(r.getSimpleField(InstanceConfigProperty.HELIX_ENABLED.toString())
                            .equals("" + false));

    // Then enable it
    paraMap.put(JsonParameters.ENABLED, "" + true);
    request = new Request(Method.POST, resourceRef);

    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(paraMap), MediaType.APPLICATION_ALL);
    client = new Client(Protocol.HTTP);
    response = client.handle(request);

    result = response.getEntity();
    sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    mapper = new ObjectMapper();
    r = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);
    AssertJUnit.assertTrue(r.getSimpleField(InstanceConfigProperty.HELIX_ENABLED.toString())
                            .equals("" + true));
  }

  void verifyAlterIdealState() throws IOException
  {
    String httpUrlBase =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/resourceGroups/"
            + resourceGroupName + "/idealState";

    Reference resourceRef = new Reference(httpUrlBase);
    Request request = new Request(Method.GET, resourceRef);

    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);

    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    ObjectMapper mapper = new ObjectMapper();
    ZNRecord r = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);
    String partitionName = "new-entity-12345_3";
    r.getMapFields().remove(partitionName);

    Map<String, String> paraMap = new HashMap<String, String>();
    // Add 1 instance
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.addIdealState);

    resourceRef = new Reference(httpUrlBase);

    request = new Request(Method.POST, resourceRef);
    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
                          + ClusterRepresentationUtil.ObjectToJson(paraMap) + "&"
                          + JsonParameters.NEW_IDEAL_STATE + "="
                          + ClusterRepresentationUtil.ZNRecordToJson(r),
                      MediaType.APPLICATION_ALL);
    client = new Client(Protocol.HTTP);
    response = client.handle(request);

    result = response.getEntity();
    sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    mapper = new ObjectMapper();
    ZNRecord r2 = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);
    AssertJUnit.assertTrue(!r2.getMapFields().containsKey(partitionName));

    for (String key : r2.getMapFields().keySet())
    {
      AssertJUnit.assertTrue(r.getMapFields().containsKey(key));
    }
  }

  // verify get/post configs in different scopes
  void verifyConfigAccessor() throws Exception
  {
    ObjectMapper mapper = new ObjectMapper();
    Client client = new Client(Protocol.HTTP);

    // set/get cluster scope configs
    String url =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/configs/cluster/"
            + clusterName;

    postConfig(client, url, mapper, ClusterSetup.setConfig, "key1=value1,key2=value2");

    ZNRecord record = get(client, url, mapper);
    Assert.assertEquals(record.getSimpleFields().size(), 2);
    Assert.assertEquals(record.getSimpleField("key1"), "value1");
    Assert.assertEquals(record.getSimpleField("key2"), "value2");

    // set/get participant scope configs
    url =
        "http://localhost:" + _port + "/clusters/" + clusterName
            + "/configs/participant/localhost_12918";

    postConfig(client, url, mapper, ClusterSetup.setConfig, "key3=value3,key4=value4");

    record = get(client, url, mapper);
    Assert.assertEquals(record.getSimpleFields().size(), 2);
    Assert.assertEquals(record.getSimpleField("key3"), "value3");
    Assert.assertEquals(record.getSimpleField("key4"), "value4");

    // set/get resource scope configs
    url =
        "http://localhost:" + _port + "/clusters/" + clusterName
            + "/configs/resource/testResource";

    postConfig(client, url, mapper, ClusterSetup.setConfig, "key5=value5,key6=value6");

    record = get(client, url, mapper);
    Assert.assertEquals(record.getSimpleFields().size(), 2);
    Assert.assertEquals(record.getSimpleField("key5"), "value5");
    Assert.assertEquals(record.getSimpleField("key6"), "value6");

    // set/get partition scope configs
    url =
        "http://localhost:" + _port + "/clusters/" + clusterName
            + "/configs/partition/testResource/testPartition";

    postConfig(client, url, mapper, ClusterSetup.setConfig, "key7=value7,key8=value8");

    record = get(client, url, mapper);
    Assert.assertEquals(record.getSimpleFields().size(), 2);
    Assert.assertEquals(record.getSimpleField("key7"), "value7");
    Assert.assertEquals(record.getSimpleField("key8"), "value8");

    // list keys
    url = "http://localhost:" + _port + "/clusters/" + clusterName + "/configs";
    record = get(client, url, mapper);
    Assert.assertEquals(record.getListFields().size(), 1);
    Assert.assertTrue(record.getListFields().containsKey("scopes"));
    Assert.assertTrue(contains(record.getListField("scopes"),
                               "CLUSTER",
                               "PARTICIPANT",
                               "RESOURCE",
                               "PARTITION"));

    url = "http://localhost:" + _port + "/clusters/" + clusterName + "/configs/cluster";
    record = get(client, url, mapper);
    Assert.assertEquals(record.getListFields().size(), 1);
    Assert.assertTrue(record.getListFields().containsKey("CLUSTER"));
    Assert.assertTrue(contains(record.getListField("CLUSTER"), clusterName));

    url =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/configs/participant";
    record = get(client, url, mapper);
    Assert.assertTrue(record.getListFields().containsKey("PARTICIPANT"));
    Assert.assertTrue(contains(record.getListField("PARTICIPANT"), "localhost_12918"));

    url = "http://localhost:" + _port + "/clusters/" + clusterName + "/configs/resource";
    record = get(client, url, mapper);
    Assert.assertEquals(record.getListFields().size(), 1);
    Assert.assertTrue(record.getListFields().containsKey("RESOURCE"));
    Assert.assertTrue(contains(record.getListField("RESOURCE"), "testResource"));

    url =
        "http://localhost:" + _port + "/clusters/" + clusterName
            + "/configs/partition/testResource";
    record = get(client, url, mapper);
    Assert.assertEquals(record.getListFields().size(), 1);
    Assert.assertTrue(record.getListFields().containsKey("PARTITION"));
    Assert.assertTrue(contains(record.getListField("PARTITION"), "testPartition"));

  }

  private ZNRecord get(Client client, String url, ObjectMapper mapper) throws Exception
  {
    Request request = new Request(Method.GET, new Reference(url));
    Response response = client.handle(request);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);
    String responseStr = sw.toString();
    Assert.assertTrue(responseStr.toLowerCase().indexOf("error") == -1);
    Assert.assertTrue(responseStr.toLowerCase().indexOf("exception") == -1);

    ZNRecord record = mapper.readValue(new StringReader(responseStr), ZNRecord.class);
    return record;
  }

  private void postConfig(Client client,
                          String url,
                          ObjectMapper mapper,
                          String command,
                          String configs) throws Exception
  {
    Map<String, String> params = new HashMap<String, String>();

    params.put(JsonParameters.MANAGEMENT_COMMAND, command);
    params.put(JsonParameters.CONFIGS, configs);

    Request request = new Request(Method.POST, new Reference(url));
    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(params), MediaType.APPLICATION_ALL);

    Response response = client.handle(request);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);
    String responseStr = sw.toString();
    Assert.assertTrue(responseStr.toLowerCase().indexOf("error") == -1);
    Assert.assertTrue(responseStr.toLowerCase().indexOf("exception") == -1);
  }

  void verifyEnableCluster() throws Exception
  {
    System.out.println("START: verifyEnableCluster()");
    String httpUrlBase =
        "http://localhost:" + _port + "/clusters/" + clusterName + "/Controller";
    Map<String, String> paramMap = new HashMap<String, String>();

    paramMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.enableCluster);
    paramMap.put(JsonParameters.ENABLED, "" + false);

    Reference resourceRef = new Reference(httpUrlBase);

    Request request = new Request(Method.POST, resourceRef);

    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(paramMap), MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);

    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    // verify pause znode exists
    String pausePath = PropertyPathConfig.getPath(PropertyType.PAUSE, clusterName);
    System.out.println("pausePath: " + pausePath);
    boolean exists = _zkclient.exists(pausePath);
    Assert.assertTrue(exists, pausePath + " should exist");

    // Then enable it
    paramMap.put(JsonParameters.ENABLED, "" + true);
    request = new Request(Method.POST, resourceRef);

    request.setEntity(JsonParameters.JSON_PARAMETERS + "="
        + ClusterRepresentationUtil.ObjectToJson(paramMap), MediaType.APPLICATION_ALL);
    client = new Client(Protocol.HTTP);
    response = client.handle(request);

    result = response.getEntity();
    sw = new StringWriter();
    result.write(sw);

    System.out.println(sw.toString());

    // verify pause znode doesn't exist
    exists = _zkclient.exists(pausePath);
    Assert.assertFalse(exists, pausePath + " should be removed");

    System.out.println("END: verifyEnableCluster()");
  }

  private boolean contains(List<String> list, String... items)
  {
    for (String item : items)
    {
      if (!list.contains(item))
      {
        return false;
      }
    }
    return true;
  }
}
