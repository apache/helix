package com.linkedin.clustermanager;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.AssertJUnit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.tools.ClusterSetup;

public class TestZKCallback extends ZkUnitTestBase
{
  private final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  
  /*
  private String _zkServerAddress;
  private List<ZkServer> _localZkServers;

  public static List<ZkServer> startLocalZookeeper(
      List<Integer> localPortsList, String zkTestDataRootDir, int tickTime)
      throws IOException
  {
    List<ZkServer> localZkServers = new ArrayList<ZkServer>();

    int count = 0;
    for (int port : localPortsList)
    {
      ZkServer zkServer = startZkServer(zkTestDataRootDir, count++, port,
          tickTime);
      localZkServers.add(zkServer);
    }
    return localZkServers;
  }

  public static ZkServer startZkServer(String zkTestDataRootDir, int machineId,
      int port, int tickTime) throws IOException
  {
    File zkTestDataRootDirFile = new File(zkTestDataRootDir);
    zkTestDataRootDirFile.mkdirs();

    String dataPath = zkTestDataRootDir + "/" + machineId + "/" + port
        + "/data";
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

    ZkServer zkServer = new ZkServer(dataPath, logPath, mockDefaultNameSpace,
        port, tickTime);
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
  */
  
  private static String[] createArgs(String str)
  {
    String[] split = str.split("[ ]+");
    System.out.println(Arrays.toString(split));
    return split;
  }

  public class TestCallbackListener implements MessageListener,
      LiveInstanceChangeListener, ConfigChangeListener,
      CurrentStateChangeListener, ExternalViewChangeListener,
      IdealStateChangeListener
  {
    boolean externalViewChangeReceived = false;
    boolean liveInstanceChangeReceived = false;
    boolean configChangeReceived = false;
    boolean currentStateChangeReceived = false;
    boolean messageChangeReceived = false;
    boolean idealStateChangeReceived = false;

    @Override
    public void onExternalViewChange(List<ZNRecord> externalViewList,
        NotificationContext changeContext)
    {
      externalViewChangeReceived = true;
    }

    @Override
    public void onStateChange(String instanceName, List<ZNRecord> statesInfo,
        NotificationContext changeContext)
    {
      currentStateChangeReceived = true;
    }

    @Override
    public void onConfigChange(List<ZNRecord> configs,
        NotificationContext changeContext)
    {
      configChangeReceived = true;
    }

    @Override
    public void onLiveInstanceChange(List<ZNRecord> liveInstances,
        NotificationContext changeContext)
    {
      liveInstanceChangeReceived = true;
    }

    @Override
    public void onMessage(String instanceName, List<ZNRecord> messages,
        NotificationContext changeContext)
    {
      messageChangeReceived = true;
    }

    void Reset()
    {
      externalViewChangeReceived = false;
      liveInstanceChangeReceived = false;
      configChangeReceived = false;
      currentStateChangeReceived = false;
      messageChangeReceived = false;
      idealStateChangeReceived = false;
    }

    @Override
    public void onIdealStateChange(List<ZNRecord> idealState,
        NotificationContext changeContext)
    {
      // TODO Auto-generated method stub
      idealStateChangeReceived = true;
    }
  }

  @Test(groups =
  { "unitTest" })
  public void testInvocation() throws Exception
  {

    ClusterManager testClusterManager = ClusterManagerFactory
        .getZKBasedManagerForParticipant(clusterName, "localhost_8900", ZK_ADDR);
    testClusterManager.connect();

    TestZKCallback test = new TestZKCallback();

    TestZKCallback.TestCallbackListener testListener = test.new TestCallbackListener();

    testClusterManager.addMessageListener(testListener, "localhost_8900");
    testClusterManager.addCurrentStateChangeListener(testListener,
        "localhost_8900", testClusterManager.getSessionId());
    testClusterManager.addConfigChangeListener(testListener);
    testClusterManager.addIdealStateChangeListener(testListener);
    testClusterManager.addExternalViewChangeListener(testListener);
    testClusterManager.addLiveInstanceChangeListener(testListener);
    // Initial add listener should trigger the first execution of the
    // listener callbacks
    AssertJUnit.assertTrue(testListener.configChangeReceived
        & testListener.currentStateChangeReceived
        & testListener.externalViewChangeReceived
        & testListener.idealStateChangeReceived
        & testListener.liveInstanceChangeReceived
        & testListener.messageChangeReceived);

    testListener.Reset();
    ClusterDataAccessor dataAccessor = testClusterManager.getDataAccessor();
    ZNRecord dummyRecord = new ZNRecord("db-12345");
    dataAccessor
        .setProperty(PropertyType.EXTERNALVIEW, dummyRecord, "db-12345" );
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.externalViewChangeReceived);
    testListener.Reset();

    dataAccessor.setProperty(PropertyType.CURRENTSTATES,dummyRecord, "localhost_8900", 
        testClusterManager.getSessionId(), dummyRecord.getId() );
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.currentStateChangeReceived);
    testListener.Reset();

    dummyRecord = new ZNRecord("db-1234");
    dataAccessor.setProperty(PropertyType.IDEALSTATES, dummyRecord,"db-1234");
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.idealStateChangeReceived);
    testListener.Reset();

    dummyRecord = new ZNRecord("db-12345");
    dataAccessor.setProperty(PropertyType.IDEALSTATES,dummyRecord, "db-12345" );
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.idealStateChangeReceived);
    testListener.Reset();

    dummyRecord = new ZNRecord("localhost:8900");
    List<ZNRecord> recList = new ArrayList<ZNRecord>();
    recList.add(dummyRecord);

    testListener.Reset();
    Message message = new Message(MessageType.STATE_TRANSITION, UUID
        .randomUUID().toString());
    message.setTgtSessionId("*");
    dataAccessor.setProperty(PropertyType.MESSAGES, message.getRecord(),
        "localhost_8900", message.getId());
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.messageChangeReceived);

    dummyRecord = new ZNRecord("localhost_9801");
    dataAccessor.setProperty(PropertyType.LIVEINSTANCES, dummyRecord,
        "localhost_9801");
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.liveInstanceChangeReceived);
    testListener.Reset();
    /*
     * dataAccessor.setNodeConfigs(recList); Thread.sleep(100);
     * AssertJUnit.assertTrue(testListener.configChangeReceived);
     * testListener.Reset();
     */

  }

  @BeforeClass(groups =
  { "unitTest" })
  public void setup() throws IOException, Exception
  {
    /*
    List<Integer> localPorts = new ArrayList<Integer>();
    localPorts.add(2300);
    localPorts.add(2301);

    _localZkServers = startLocalZookeeper(localPorts,
        System.getProperty("user.dir") + "/" + "zkdata", 2000);
    _zkServerAddress = "localhost:" + 2300;
    */
    if (_zkClient.exists("/" + clusterName))
    {
      _zkClient.deleteRecursive("/" + clusterName);
    }
    
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addCluster " + clusterName));
    // ClusterSetup
    //    .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addCluster relay-cluster-12345"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addResourceGroup " + clusterName + " db-12345 120 MasterSlave"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName + " localhost:8900"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName + " localhost:8901"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName + " localhost:8902"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName + " localhost:8903"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -addNode " + clusterName + " localhost:8904"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr " + ZK_ADDR + " -rebalance " + clusterName + " db-12345 3"));
  }

  @AfterMethod
  @AfterClass(groups =
  { "unitTest" })
  public void tearDown()
  {
    // stopLocalZookeeper(_localZkServers);
  }

}
