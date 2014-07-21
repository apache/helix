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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.util.HelixUtil;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestClusterSetup extends ZkTestBase {
  protected static final String CLUSTER_NAME = "TestClusterSetup";
  protected static final String TEST_DB = "TestDB";
  protected static final String INSTANCE_PREFIX = "instance_";
  protected static final String STATE_MODEL = "MasterSlave";
  protected static final String TEST_NODE = "testnode_1";

  private static String[] createArgs(String str) {
    String[] split = str.split("[ ]+");
    System.out.println(Arrays.toString(split));
    return split;
  }

  @BeforeClass()
  public void beforeClass() throws IOException, Exception {
    System.out.println("START TestClusterSetup.beforeClass() "
        + new Date(System.currentTimeMillis()));
  }

  @AfterClass()
  public void afterClass() {
    System.out.println("END TestClusterSetup.afterClass() " + new Date(System.currentTimeMillis()));
  }

  @BeforeMethod()
  public void setup() {

    _zkclient.deleteRecursive("/" + CLUSTER_NAME);
    _setupTool.addCluster(CLUSTER_NAME, true);
  }

  private void verifyEnabled(ZkClient zkClient, String clusterName, String instance,
      boolean wantEnabled) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instance));
    AssertJUnit.assertEquals(wantEnabled, config.getInstanceEnabled());
  }

  private void verifyInstance(ZkClient zkClient, String clusterName, String instance,
      boolean wantExists) {
    // String instanceConfigsPath = HelixUtil.getConfigPath(clusterName);
    String instanceConfigsPath =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString());
    String instanceConfigPath = instanceConfigsPath + "/" + instance;
    String instancePath = HelixUtil.getInstancePath(clusterName, instance);
    AssertJUnit.assertEquals(wantExists, zkClient.exists(instanceConfigPath));
    AssertJUnit.assertEquals(wantExists, zkClient.exists(instancePath));
  }

  private void verifyResource(ZkClient zkClient, String clusterName, String resource,
      boolean wantExists) {
    String resourcePath = HelixUtil.getIdealStatePath(clusterName) + "/" + resource;
    AssertJUnit.assertEquals(wantExists, zkClient.exists(resourcePath));
  }

  private void verifyReplication(ZkClient zkClient, String clusterName, String resource, int repl) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resource));
    for (PartitionId partitionId : idealState.getPartitionIdSet()) {
      if (idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
        AssertJUnit.assertEquals(repl, idealState.getPreferenceList(partitionId).size());
      } else if (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
        AssertJUnit.assertEquals(repl, idealState.getParticipantStateMap(partitionId).size());
      }
    }
  }

  @Test
  public void testZkAdminTimeout() {
    boolean exceptionThrown = false;
    try {
      new ZKHelixAdmin("localhost:27999");
    } catch (Exception e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    System.setProperty(ZKHelixAdmin.CONNECTION_TIMEOUT, "3");
    exceptionThrown = false;
    long time = System.currentTimeMillis();
    try {
      new ZKHelixAdmin("localhost:27999");
    } catch (Exception e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    Assert.assertTrue(System.currentTimeMillis() - time < 5000);
  }

  @Test()
  public void testAddInstancesToCluster() throws Exception {
    String instanceAddresses[] = new String[3];
    for (int i = 0; i < 3; i++) {
      String currInstance = INSTANCE_PREFIX + i;
      instanceAddresses[i] = currInstance;
    }
    String nextInstanceAddress = INSTANCE_PREFIX + 3;

    _setupTool.addInstancesToCluster(CLUSTER_NAME, instanceAddresses);

    // verify instances
    for (String instance : instanceAddresses) {
      verifyInstance(_zkclient, CLUSTER_NAME, instance, true);
    }

    _setupTool.addInstanceToCluster(CLUSTER_NAME, nextInstanceAddress);
    verifyInstance(_zkclient, CLUSTER_NAME, nextInstanceAddress, true);
    // re-add
    boolean caughtException = false;
    try {
      _setupTool.addInstanceToCluster(CLUSTER_NAME, nextInstanceAddress);
    } catch (HelixException e) {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test()
  public void testDisableDropInstancesFromCluster() throws Exception {
    testAddInstancesToCluster();
    String instanceAddresses[] = new String[3];
    for (int i = 0; i < 3; i++) {
      String currInstance = INSTANCE_PREFIX + i;
      instanceAddresses[i] = currInstance;
    }
    String nextInstanceAddress = INSTANCE_PREFIX + 3;

    boolean caughtException = false;
    // drop without disabling
    try {
      _setupTool.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
    } catch (HelixException e) {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);

    // disable
    _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, nextInstanceAddress,
        false);
    verifyEnabled(_zkclient, CLUSTER_NAME, nextInstanceAddress, false);

    // drop
    _setupTool.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
    verifyInstance(_zkclient, CLUSTER_NAME, nextInstanceAddress, false);

    // re-drop
    caughtException = false;
    try {
      _setupTool.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
    } catch (HelixException e) {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
    /*
     * //drop a set setupTool.getClusterManagementTool().enableInstances(CLUSTER_NAME,
     * instanceAddresses, false); setupTool.dropInstancesFromCluster(CLUSTER_NAME,
     * instanceAddresses);
     */

    // bad format disable, drop
    String badFormatInstance = "badinstance";
    caughtException = false;
    try {
      _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, badFormatInstance,
          false);
    } catch (HelixException e) {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);

    caughtException = false;
    try {
      _setupTool.dropInstanceFromCluster(CLUSTER_NAME, badFormatInstance);
    } catch (HelixException e) {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test()
  public void testAddResource() throws Exception {
    try {
      _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, 16, STATE_MODEL);
    } catch (Exception e) {
    }
    verifyResource(_zkclient, CLUSTER_NAME, TEST_DB, true);
  }

  @Test()
  public void testRemoveResource() throws Exception {
    _setupTool.setupTestCluster(CLUSTER_NAME);
    verifyResource(_zkclient, CLUSTER_NAME, TEST_DB, true);
    _setupTool.dropResourceFromCluster(CLUSTER_NAME, TEST_DB);
    verifyResource(_zkclient, CLUSTER_NAME, TEST_DB, false);
  }

  @Test()
  public void testRebalanceCluster() throws Exception {
    _setupTool.setupTestCluster(CLUSTER_NAME);
    // testAddInstancesToCluster();
    testAddResource();
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 4);
    verifyReplication(_zkclient, CLUSTER_NAME, TEST_DB, 4);
  }

  /*
   * @Test (groups = {"unitTest"}) public void testPrintUsage() throws Exception { Options
   * cliOptions = ClusterSetup.constructCommandLineOptions();
   * ClusterSetup.printUsage(null); }
   */

  @Test()
  public void testParseCommandLinesArgs() throws Exception {
    // ClusterSetup
    // .processCommandLineArgs(createArgs("-zkSvr "+zkaddr+ " help"));

    // wipe ZK
    _zkclient.deleteRecursive("/" + CLUSTER_NAME);

    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + _zkaddr + " --addCluster "
        + CLUSTER_NAME));

    // wipe again
    _zkclient.deleteRecursive("/" + CLUSTER_NAME);

    _setupTool.setupTestCluster(CLUSTER_NAME);

    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + _zkaddr + " --addNode "
        + CLUSTER_NAME + " " + TEST_NODE));
    verifyInstance(_zkclient, CLUSTER_NAME, TEST_NODE, true);
    try {
      ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + _zkaddr + " --addResource "
          + CLUSTER_NAME + " " + TEST_DB + " 4 " + STATE_MODEL));
    } catch (Exception e) {

    }
    verifyResource(_zkclient, CLUSTER_NAME, TEST_DB, true);
    // ClusterSetup
    // .processCommandLineArgs(createArgs("-zkSvr "+zkaddr+" --addNode node-1"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + _zkaddr + " --enableInstance "
        + CLUSTER_NAME + " " + TEST_NODE + " true"));
    verifyEnabled(_zkclient, CLUSTER_NAME, TEST_NODE, true);

    // TODO: verify list commands
    /*
     * ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+zkaddr+" --listClusterInfo "
     * +CLUSTER_NAME)); ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+zkaddr+" --listClusters"));
     * ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+zkaddr+" --listInstanceInfo "
     * +CLUSTER_NAME+" "+instanceColonToUnderscoreFormat(TEST_NODE))); ClusterSetup
     * .processCommandLineArgs
     * (createArgs("-zkSvr "+zkaddr+" --listInstances "+CLUSTER_NAME)); ClusterSetup
     * .processCommandLineArgs
     * (createArgs("-zkSvr "+zkaddr+" --listResourceInfo "+CLUSTER_NAME +" "+TEST_DB));
     * ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+zkaddr+" --listResources "
     * +CLUSTER_NAME)); ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "+zkaddr+" --listStateModel "
     * +CLUSTER_NAME+" "+STATE_MODEL)); ClusterSetup
     * .processCommandLineArgs(createArgs("-zkSvr "
     * +zkaddr+" --listStateModels "+CLUSTER_NAME));
     */
    // ClusterSetup
    // .processCommandLineArgs(createArgs("-zkSvr "+zkaddr+" --rebalance "+CLUSTER_NAME+" "+TEST_DB+" 1"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + _zkaddr + " --enableInstance "
        + CLUSTER_NAME + " " + TEST_NODE + " false"));
    verifyEnabled(_zkclient, CLUSTER_NAME, TEST_NODE, false);
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + _zkaddr + " --dropNode "
        + CLUSTER_NAME + " " + TEST_NODE));
  }

  @Test()
  public void testSetGetRemoveParticipantConfig() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    _setupTool.addCluster(clusterName, true);
    _setupTool.addInstanceToCluster(clusterName, "localhost_0");

    // test set/get/remove instance configs
    String scopeArgs = clusterName + ",localhost_0";
    String keyValueMap = "key1=value1,key2=value2";
    String keys = "key1,key2";
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--setConfig", ConfigScopeProperty.PARTICIPANT.toString(), scopeArgs,
        keyValueMap
    });

    // getConfig returns json-formatted key-value pairs
    String valuesStr = _setupTool.getConfig(ConfigScopeProperty.PARTICIPANT, scopeArgs, keys);
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    ZNRecord record = (ZNRecord) serializer.deserialize(valuesStr.getBytes());
    Assert.assertEquals(record.getSimpleField("key1"), "value1");
    Assert.assertEquals(record.getSimpleField("key2"), "value2");

    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--removeConfig", ConfigScopeProperty.PARTICIPANT.toString(),
        scopeArgs, keys
    });
    valuesStr = _setupTool.getConfig(ConfigScopeProperty.PARTICIPANT, scopeArgs, keys);
    record = (ZNRecord) serializer.deserialize(valuesStr.getBytes());
    Assert.assertNull(record.getSimpleField("key1"));
    Assert.assertNull(record.getSimpleField("key2"));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testEnableCluster() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // pause cluster
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--enableCluster", clusterName, "false"
    });

    Builder keyBuilder = new Builder(clusterName);
    boolean exists = _zkclient.exists(keyBuilder.pause().getPath());
    Assert.assertTrue(exists, "pause node under controller should be created");

    // resume cluster
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--enableCluster", clusterName, "true"
    });

    exists = _zkclient.exists(keyBuilder.pause().getPath());
    Assert.assertFalse(exists, "pause node under controller should be removed");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testDropInstance() throws Exception {
    // drop without stop, should throw exception
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // add fake liveInstance
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkclient));
    Builder keyBuilder = new Builder(clusterName);
    LiveInstance liveInstance = new LiveInstance("localhost_12918");
    liveInstance.setSessionId("session_0");
    liveInstance.setHelixVersion("version_0");
    accessor.setProperty(keyBuilder.liveInstance("localhost_12918"), liveInstance);

    // drop without stop the process, should throw exception
    try {
      ClusterSetup.processCommandLineArgs(new String[] {
          "--zkSvr", _zkaddr, "--dropNode", clusterName, "localhost:12918"
      });
      Assert.fail("Should throw exception since localhost_12918 is still in LIVEINSTANCES/");
    } catch (Exception e) {
      // OK
    }
    accessor.removeProperty(keyBuilder.liveInstance("localhost_12918"));

    // drop without disable, should throw exception
    try {
      ClusterSetup.processCommandLineArgs(new String[] {
          "--zkSvr", _zkaddr, "--dropNode", clusterName, "localhost:12918"
      });
      Assert.fail("Should throw exception since localhost_12918 is enabled");
    } catch (Exception e) {
      // e.printStackTrace();
      // OK
    }

    // drop it
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--enableInstance", clusterName, "localhost_12918", "false"
    });
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--dropNode", clusterName, "localhost:12918"
    });

    Assert.assertNull(accessor.getProperty(keyBuilder.instanceConfig("localhost_12918")),
        "Instance config should be dropped");
    Assert.assertFalse(_zkclient.exists(PropertyPathConfig.getPath(PropertyType.INSTANCES,
        clusterName, "localhost_12918")), "Instance/host should be dropped");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testDisableResource() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance
    // disable "TestDB0" resource
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--enableResource", clusterName, "TestDB0", "false"
    });
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    Assert.assertFalse(idealState.isEnabled());
    // enable "TestDB0" resource
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--enableResource", clusterName, "TestDB0", "true"
    });
    idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    Assert.assertTrue(idealState.isEnabled());
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

}
