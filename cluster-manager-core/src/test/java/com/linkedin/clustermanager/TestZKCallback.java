package com.linkedin.clustermanager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.tools.ClusterSetup;

// TODO inherit from ZkTestBase
@Test (groups = {"unitTest"})
public class TestZKCallback
{
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
      // TODO Auto-generated method stub
      externalViewChangeReceived = true;
    }

    @Override
    public void onStateChange(String instanceName, List<ZNRecord> statesInfo,
        NotificationContext changeContext)
    {
      // TODO Auto-generated method stub
      currentStateChangeReceived = true;
    }

    @Override
    public void onConfigChange(List<ZNRecord> configs,
        NotificationContext changeContext)
    {
      // TODO Auto-generated method stub
      configChangeReceived = true;
    }

    @Override
    public void onLiveInstanceChange(List<ZNRecord> liveInstances,
        NotificationContext changeContext)
    {
      // TODO Auto-generated method stub
      liveInstanceChangeReceived = true;
    }

    @Override
    public void onMessage(String instanceName, List<ZNRecord> messages,
        NotificationContext changeContext)
    {
      // TODO Auto-generated method stub
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

  @Test
  public void testInvocation() throws Exception
  {

    ClusterManager testClusterManager = ClusterManagerFactory
        .getZKBasedManagerForParticipant("storage-cluster-12345",
            "localhost_8900", _zkServerAddress);
    testClusterManager.connect();

    TestZKCallback test = new TestZKCallback();

    TestZKCallback.TestCallbackListener testListener = test.new TestCallbackListener();

    testClusterManager.addMessageListener(testListener, "localhost_8900");
    testClusterManager.addCurrentStateChangeListener(testListener,
        "localhost_8900",testClusterManager.getSessionId());
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
    dataAccessor.setClusterProperty(ClusterPropertyType.EXTERNALVIEW,
        "db-12345", dummyRecord);
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.externalViewChangeReceived);
    testListener.Reset();

    dataAccessor.setInstanceProperty("localhost_8900",
        InstancePropertyType.CURRENTSTATES, testClusterManager.getSessionId(),dummyRecord.getId(), dummyRecord);
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.currentStateChangeReceived);
    testListener.Reset();

    dummyRecord = new ZNRecord("db-1234");
    dataAccessor.setClusterProperty(ClusterPropertyType.IDEALSTATES, "db-1234",
        dummyRecord);
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.idealStateChangeReceived);
    testListener.Reset();

    dummyRecord = new ZNRecord("db-12345");
    dataAccessor.setClusterProperty(ClusterPropertyType.IDEALSTATES,
        "db-12345", dummyRecord);
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.idealStateChangeReceived);
    testListener.Reset();
   
    dummyRecord = new ZNRecord("localhost:8900");
    List<ZNRecord> recList = new ArrayList<ZNRecord>();
    recList.add(dummyRecord);

    testListener.Reset();
    Message message = new Message(MessageType.STATE_TRANSITION,UUID.randomUUID().toString());
    message.setTgtSessionId("*");
    dataAccessor.setInstanceProperty("localhost_8900",
        InstancePropertyType.MESSAGES, message.getId(), message.getRecord());
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.messageChangeReceived);

    dummyRecord = new ZNRecord("localhost_9801");
    dataAccessor.setClusterProperty(ClusterPropertyType.LIVEINSTANCES,
        "localhost_9801", dummyRecord);
    Thread.sleep(100);
    AssertJUnit.assertTrue(testListener.liveInstanceChangeReceived);
    testListener.Reset();
    /*
     * dataAccessor.setNodeConfigs(recList); Thread.sleep(100);
     * AssertJUnit.assertTrue(testListener.configChangeReceived);
     * testListener.Reset();
     */

  }

  @BeforeTest
  public void setup() throws IOException, Exception
  {
    List<Integer> localPorts = new ArrayList<Integer>();
    localPorts.add(2300);
    localPorts.add(2301);

    _localZkServers = startLocalZookeeper(localPorts,
        System.getProperty("user.dir") + "/" + "zkdata", 2000);
    _zkServerAddress = "localhost:" + 2300;

    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2300 -addCluster storage-cluster-12345"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2300 -addCluster relay-cluster-12345"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2300 -addResourceGroup storage-cluster-12345 db-12345 120 MasterSlave"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2300 -addNode storage-cluster-12345 localhost:8900"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2300 -addNode storage-cluster-12345 localhost:8901"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2300 -addNode storage-cluster-12345 localhost:8902"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2300 -addNode storage-cluster-12345 localhost:8903"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2300 -addNode storage-cluster-12345 localhost:8904"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2300 -rebalance storage-cluster-12345 db-12345 3"));
  }

  @AfterTest
  public void tearDown()
  {
    stopLocalZookeeper(_localZkServers);
  }

}
