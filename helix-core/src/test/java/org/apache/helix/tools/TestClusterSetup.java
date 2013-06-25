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

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestClusterSetup extends ZkUnitTestBase
{
  private static Logger         LOG             =
                                                    Logger.getLogger(TestClusterSetup.class);

  protected static final String CLUSTER_NAME    = "TestClusterSetup";
  protected static final String TEST_DB         = "TestDB";
  protected static final String INSTANCE_PREFIX = "instance_";
  protected static final String STATE_MODEL     = "MasterSlave";
  protected static final String TEST_NODE       = "testnode_1";

  ZkClient                      _zkClient;
  ClusterSetup                  _clusterSetup;

  private static String[] createArgs(String str)
  {
    String[] split = str.split("[ ]+");
    System.out.println(Arrays.toString(split));
    return split;
  }

  @BeforeClass()
  public void beforeClass() throws IOException,
      Exception
  {
    System.out.println("START TestClusterSetup.beforeClass() "
        + new Date(System.currentTimeMillis()));

    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
  }

  @AfterClass()
  public void afterClass()
  {
    _zkClient.close();
    System.out.println("END TestClusterSetup.afterClass() "
        + new Date(System.currentTimeMillis()));
  }

  @BeforeMethod()
  public void setup()
  {

    _zkClient.deleteRecursive("/" + CLUSTER_NAME);
    _clusterSetup = new ClusterSetup(ZK_ADDR);
    _clusterSetup.addCluster(CLUSTER_NAME, true);
  }
  
  @Test
  public void testZkAdminTimeout()
  {
    boolean exceptionThrown = false;
    try
    {
      new ZKHelixAdmin("localhost:27999");
    }
    catch(Exception e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    System.setProperty(ZKHelixAdmin.CONNECTION_TIMEOUT, "3");
    exceptionThrown = false;
    long time = System.currentTimeMillis();
    try
    {
      new ZKHelixAdmin("localhost:27999");
    }
    catch(Exception e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    Assert.assertTrue(System.currentTimeMillis() - time  < 5000);
  }
  
  @Test()
  public void testAddInstancesToCluster() throws Exception
  {
    String instanceAddresses[] = new String[3];
    for (int i = 0; i < 3; i++)
    {
      String currInstance = INSTANCE_PREFIX + i;
      instanceAddresses[i] = currInstance;
    }
    String nextInstanceAddress = INSTANCE_PREFIX + 3;

    _clusterSetup.addInstancesToCluster(CLUSTER_NAME, instanceAddresses);

    // verify instances
    for (String instance : instanceAddresses)
    {
      verifyInstance(_zkClient,
                     CLUSTER_NAME,
                     instance,
                     true);
    }

    _clusterSetup.addInstanceToCluster(CLUSTER_NAME, nextInstanceAddress);
    verifyInstance(_zkClient,
                   CLUSTER_NAME,
                   nextInstanceAddress,
                   true);
    // re-add
    boolean caughtException = false;
    try
    {
      _clusterSetup.addInstanceToCluster(CLUSTER_NAME, nextInstanceAddress);
    }
    catch (HelixException e)
    {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test()
  public void testDisableDropInstancesFromCluster() throws Exception
  {
    testAddInstancesToCluster();
    String instanceAddresses[] = new String[3];
    for (int i = 0; i < 3; i++)
    {
      String currInstance = INSTANCE_PREFIX + i;
      instanceAddresses[i] = currInstance;
    }
    String nextInstanceAddress = INSTANCE_PREFIX + 3;

    boolean caughtException = false;
    // drop without disabling
    try
    {
      _clusterSetup.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
    }
    catch (HelixException e)
    {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);

    // disable
    _clusterSetup.getClusterManagementTool()
                 .enableInstance(CLUSTER_NAME,
                                 nextInstanceAddress,
                                 false);
    verifyEnabled(_zkClient,
                  CLUSTER_NAME,
                  nextInstanceAddress,
                  false);

    // drop
    _clusterSetup.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
    verifyInstance(_zkClient,
                   CLUSTER_NAME,
                   nextInstanceAddress,
                   false);

    // re-drop
    caughtException = false;
    try
    {
      _clusterSetup.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
    }
    catch (HelixException e)
    {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
    /*
     * //drop a set _clusterSetup.getClusterManagementTool().enableInstances(CLUSTER_NAME,
     * instanceAddresses, false); _clusterSetup.dropInstancesFromCluster(CLUSTER_NAME,
     * instanceAddresses);
     */

    // bad format disable, drop
    String badFormatInstance = "badinstance";
    caughtException = false;
    try
    {
      _clusterSetup.getClusterManagementTool().enableInstance(CLUSTER_NAME,
                                                              badFormatInstance,
                                                              false);
    }
    catch (HelixException e)
    {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);

    caughtException = false;
    try
    {
      _clusterSetup.dropInstanceFromCluster(CLUSTER_NAME, badFormatInstance);
    }
    catch (HelixException e)
    {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test()
  public void testAddResource() throws Exception
  {
    try
    {
      _clusterSetup.addResourceToCluster(CLUSTER_NAME, TEST_DB, 16, STATE_MODEL);
    }
    catch(Exception e)
    {}
    verifyResource(_zkClient, CLUSTER_NAME, TEST_DB, true);
  }

  @Test()
  public void testRemoveResource() throws Exception
  {
    _clusterSetup.setupTestCluster(CLUSTER_NAME);
    verifyResource(_zkClient, CLUSTER_NAME, TEST_DB, true);
    _clusterSetup.dropResourceFromCluster(CLUSTER_NAME, TEST_DB);
    verifyResource(_zkClient, CLUSTER_NAME, TEST_DB, false);
  }

  @Test()
  public void testRebalanceCluster() throws Exception
  {
    _clusterSetup.setupTestCluster(CLUSTER_NAME);
    // testAddInstancesToCluster();
    testAddResource();
    _clusterSetup.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 4);
    verifyReplication(_zkClient, CLUSTER_NAME, TEST_DB, 4);
  }

  /*
   * @Test (groups = {"unitTest"}) public void testPrintUsage() throws Exception { Options
   * cliOptions = ClusterSetup.constructCommandLineOptions();
   * ClusterSetup.printUsage(null); }
   */

  @Test()
  public void testParseCommandLinesArgs() throws Exception
  {
    // ClusterSetup
    // .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+ " help"));

    // wipe ZK
    _zkClient.deleteRecursive("/" + CLUSTER_NAME);
    _clusterSetup = new ClusterSetup(ZK_ADDR);

    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " --addCluster "
        + CLUSTER_NAME));

    // wipe again
    _zkClient.deleteRecursive("/" + CLUSTER_NAME);
    _clusterSetup = new ClusterSetup(ZK_ADDR);

    _clusterSetup.setupTestCluster(CLUSTER_NAME);

    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " --addNode "
        + CLUSTER_NAME + " " + TEST_NODE));
    verifyInstance(_zkClient,
                   CLUSTER_NAME,
                   TEST_NODE,
                   true);
    try
    {
      ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR
        + " --addResource " + CLUSTER_NAME + " " + TEST_DB + " 4 " + STATE_MODEL));
    }
    catch(Exception e)
    {
      
    }
    verifyResource(_zkClient, CLUSTER_NAME, TEST_DB, true);
    // ClusterSetup
    // .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --addNode node-1"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR
        + " --enableInstance " + CLUSTER_NAME + " "
        + TEST_NODE + " true"));
    verifyEnabled(_zkClient,
                  CLUSTER_NAME,
                  TEST_NODE,
                  true);

    // TODO: verify list commands
    /*
     * ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listClusterInfo "
     * +CLUSTER_NAME)); ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listClusters"));
     * ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listInstanceInfo "
     * +CLUSTER_NAME+" "+instanceColonToUnderscoreFormat(TEST_NODE))); ClusterSetup
     * .processCommandLineArgs
     * (createArgs("-zkSvr "+ZK_ADDR+" --listInstances "+CLUSTER_NAME)); ClusterSetup
     * .processCommandLineArgs
     * (createArgs("-zkSvr "+ZK_ADDR+" --listResourceInfo "+CLUSTER_NAME +" "+TEST_DB));
     * ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listResources "
     * +CLUSTER_NAME)); ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listStateModel "
     * +CLUSTER_NAME+" "+STATE_MODEL)); ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "
     * +ZK_ADDR+" --listStateModels "+CLUSTER_NAME));
     */
    // ClusterSetup
    // .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --rebalance "+CLUSTER_NAME+" "+TEST_DB+" 1"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR
        + " --enableInstance " + CLUSTER_NAME + " "
        + TEST_NODE + " false"));
    verifyEnabled(_zkClient,
                  CLUSTER_NAME,
                  TEST_NODE,
                  false);
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " --dropNode "
        + CLUSTER_NAME + " " + TEST_NODE));
  }

  @Test()
  public void testSetGetConfig() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    // basic
    _clusterSetup.addCluster(clusterName, true);
    _clusterSetup.addInstanceToCluster(clusterName, "localhost_0");
    String scopeArgs = clusterName + ",localhost_0";
    String keyValueMap = "key1=value1,key2=value2";
    String keys = "key1,key2";
    _clusterSetup.setConfig(ConfigScopeProperty.PARTICIPANT, scopeArgs, keyValueMap);
    String valuesStr = _clusterSetup.getConfig(ConfigScopeProperty.PARTICIPANT, scopeArgs, keys);
    
    // getConfig returns json-formatted key-value pairs
    ZNRecord record = new ZNRecord(ConfigScopeProperty.PARTICIPANT.toString());
    // record.setMapField(scopesStr,HelixUtil.parseCsvFormatedKeyValuePairs(propertiesStr));
    record.getSimpleFields().putAll(HelixUtil.parseCsvFormatedKeyValuePairs(keyValueMap));
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    Assert.assertEquals(valuesStr, new String(serializer.serialize(record)));

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testEnableCluster() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // pause cluster
    ClusterSetup.processCommandLineArgs(new String[] { "--zkSvr", ZK_ADDR,
        "--enableCluster", clusterName, "false" });

    Builder keyBuilder = new Builder(clusterName);
    boolean exists = _gZkClient.exists(keyBuilder.pause().getPath());
    Assert.assertTrue(exists, "pause node under controller should be created");

    // resume cluster
    ClusterSetup.processCommandLineArgs(new String[] { "--zkSvr", ZK_ADDR,
        "--enableCluster", clusterName, "true" });

    exists = _gZkClient.exists(keyBuilder.pause().getPath());
    Assert.assertFalse(exists, "pause node under controller should be removed");

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testDropInstance() throws Exception
  {
    // drop without stop, should throw exception
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // add fake liveInstance
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = new Builder(clusterName);
    LiveInstance liveInstance = new LiveInstance("localhost_12918");
    liveInstance.setSessionId("session_0");
    liveInstance.setHelixVersion("version_0");
    accessor.setProperty(keyBuilder.liveInstance("localhost_12918"), liveInstance);

    // drop without stop the process, should throw exception
    try
    {
      ClusterSetup.processCommandLineArgs(new String[] { "--zkSvr", ZK_ADDR,
          "--dropNode", clusterName, "localhost:12918" });
      Assert.fail("Should throw exception since localhost_12918 is still in LIVEINSTANCES/");
    }
    catch (Exception e)
    {
      // OK
    }
    accessor.removeProperty(keyBuilder.liveInstance("localhost_12918"));

    // drop without disable, should throw exception
    try
    {
      ClusterSetup.processCommandLineArgs(new String[] { "--zkSvr", ZK_ADDR,
          "--dropNode", clusterName, "localhost:12918" });
      Assert.fail("Should throw exception since localhost_12918 is enabled");
    }
    catch (Exception e)
    {
      // e.printStackTrace();
      // OK
    }

    // drop it
    ClusterSetup.processCommandLineArgs(new String[] { "--zkSvr", ZK_ADDR,
        "--enableInstance", clusterName, "localhost_12918", "false" });
    ClusterSetup.processCommandLineArgs(new String[] { "--zkSvr", ZK_ADDR, "--dropNode",
        clusterName, "localhost:12918" });

    Assert.assertNull(accessor.getProperty(keyBuilder.instanceConfig("localhost_12918")),
                      "Instance config should be dropped");
    Assert.assertFalse(_gZkClient.exists(PropertyPathConfig.getPath(PropertyType.INSTANCES,
                                                                    clusterName,
                                                                    "localhost_12918")),
                       "Instance/host should be dropped");
    
    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }
  
}
