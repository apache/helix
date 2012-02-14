package com.linkedin.helix;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;

public class TestClusterSetup extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestClusterSetup.class);

  protected static final String CLUSTER_NAME = "TestCluster";
  protected static final String TEST_DB = "TestDB";
  protected static final String INSTANCE_PREFIX = "instance:";
  protected static final String STATE_MODEL = "MasterSlave";
  protected static final String TEST_NODE = "testnode:1";

  ZkClient _zkClient;
  ClusterSetup _clusterSetup;

  String instanceColonToUnderscoreFormat(String colonFormat)
  {
    int lastPos = colonFormat.lastIndexOf(":");
    if (lastPos <= 0)
    {
      String error = "Invalid storage Instance info format: " + colonFormat;
      LOG.warn(error);
      throw new HelixException(error);
    }
    String host = colonFormat.substring(0, lastPos);
    String portStr = colonFormat.substring(lastPos + 1);
    return host + "_" + portStr;
  }

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
                     instanceColonToUnderscoreFormat(instance),
                     true);
    }

    _clusterSetup.addInstanceToCluster(CLUSTER_NAME, nextInstanceAddress);
    verifyInstance(_zkClient,
                   CLUSTER_NAME,
                   instanceColonToUnderscoreFormat(nextInstanceAddress),
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

    // bad instance format
    String badFormatInstance = "badinstance";
    caughtException = false;
    try
    {
      _clusterSetup.addInstanceToCluster(CLUSTER_NAME, badFormatInstance);
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
                                 instanceColonToUnderscoreFormat(nextInstanceAddress),
                                 false);
    verifyEnabled(_zkClient,
                  CLUSTER_NAME,
                  instanceColonToUnderscoreFormat(nextInstanceAddress),
                  false);

    // drop
    _clusterSetup.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
    verifyInstance(_zkClient,
                   CLUSTER_NAME,
                   instanceColonToUnderscoreFormat(nextInstanceAddress),
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
    _clusterSetup.addResourceToCluster(CLUSTER_NAME, TEST_DB, 16, STATE_MODEL);
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
                   instanceColonToUnderscoreFormat(TEST_NODE),
                   true);
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR
        + " --addResource " + CLUSTER_NAME + " " + TEST_DB + " 4 " + STATE_MODEL));
    verifyResource(_zkClient, CLUSTER_NAME, TEST_DB, true);
    // ClusterSetup
    // .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --addNode node-1"));
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR
        + " --enableInstance " + CLUSTER_NAME + " "
        + instanceColonToUnderscoreFormat(TEST_NODE) + " true"));
    verifyEnabled(_zkClient,
                  CLUSTER_NAME,
                  instanceColonToUnderscoreFormat(TEST_NODE),
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
     * (createArgs("-zkSvr "+ZK_ADDR+" --listResourceInfo "+CLUSTER_NAME
     * +" "+TEST_DB)); ClusterSetup
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
        + instanceColonToUnderscoreFormat(TEST_NODE) + " false"));
    verifyEnabled(_zkClient,
                  CLUSTER_NAME,
                  instanceColonToUnderscoreFormat(TEST_NODE),
                  false);
    ClusterSetup.processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " --dropNode "
        + CLUSTER_NAME + " " + TEST_NODE));
  }
}
