package org.apache.helix.common.execution;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.ZkServer;


/**
 * A placeholder class for test execution runtime variables.
 */
public class TestExecutionRuntime {

  public static final String MULTI_ZK_PROPERTY_KEY = "multiZk";
  public static final String NUM_ZK_PROPERTY_KEY = "numZk";
  public static final String ZK_PREFIX = "localhost:";
  public static final int ZK_START_PORT = 2183;
  public static final long MANUAL_GC_PAUSE = 4000L;
  public static final String ZK_ADDR = ZK_PREFIX + ZK_START_PORT;

  protected ZkServer _zkServer;
  protected HelixZkClient _gZkClient;
  protected ClusterSetup _gSetupTool;
  protected BaseDataAccessor<ZNRecord> _baseAccessor;
  protected MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();
  protected Map<String, Map<String, HelixZkClient>> _liveInstanceOwners = new HashMap<>();

  /*
   * Multiple ZK references
   */
  // The following maps hold ZK connect string as keys
  private static final Map<String, ZkServer> _zkServerMap = new HashMap<>();
  private static final Map<String, HelixZkClient> _helixZkClientMap = new HashMap<>();
  private static final Map<String, ClusterSetup> _clusterSetupMap = new HashMap<>();
  private static final Map<String, BaseDataAccessor> _baseDataAccessorMap = new HashMap<>();

  protected void initialize() {
    initialize(1);
  }

  protected void initialize(int numberOfZK) {
    // TODO: use logging.properties file to config java.util.logging.Logger levels
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);

    // Due to ZOOKEEPER-2693 fix, we need to specify whitelist for execute zk commends
    System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    System.setProperty(SystemPropertyKeys.CONTROLLER_MESSAGE_PURGE_DELAY, "3000");

    // Start in-memory ZooKeepers. If multi-ZooKeeper is enabled, start more ZKs. Otherwise, just set up one ZK
    AtomicReference<Integer> numZkToStart = new AtomicReference<>(numberOfZK);
    Optional.ofNullable(System.getProperty(MULTI_ZK_PROPERTY_KEY))
        .map(Boolean.TRUE.toString()::equalsIgnoreCase)
        .ifPresent(b -> {
          Integer numZKFromConfig = Optional.ofNullable(System.getProperty(NUM_ZK_PROPERTY_KEY))
              .map(Integer::parseInt)
              .orElseThrow(() -> new RuntimeException("multiZk config is set but numZk config is missing!"));

          numZkToStart.set(Math.max(numZkToStart.get(), numZKFromConfig));
        });

    // Start "numZkFromConfigInt" ZooKeepers
    for (int i = 0; i < numZkToStart.get(); i++) {
      startZooKeeper(i);
    }

    // Set the references for backward-compatibility with a single ZK environment
    _zkServer = _zkServerMap.get(ZK_ADDR);
    _gZkClient = _helixZkClientMap.get(ZK_ADDR);
    _gSetupTool = _clusterSetupMap.get(ZK_ADDR);
    _baseAccessor = _baseDataAccessorMap.get(ZK_ADDR);
    cleanupJMXObjects();
  }

  /**
   * Unwinds the Test Execution Runtime.
   */
  public void unwind() {
    cleanupJMXObjects();
    synchronized (TestExecutionRuntime.class) {
      // Close all ZK resources
      _baseDataAccessorMap.values().forEach(BaseDataAccessor::close);
      _clusterSetupMap.values().forEach(ClusterSetup::close);
      _helixZkClientMap.values().forEach(HelixZkClient::close);
      _zkServerMap.values().forEach(TestHelper::stopZkServer);
    }
  }

  /**
   * Clean up all JMX beans from the MBean Server.
   */
  public void cleanupJMXObjects() {
    try {
      // Clean up all JMX objects
      for (ObjectName mbean : _server.queryNames(null, null)) {
        _server.unregisterMBean(mbean);
      }
    } catch (Exception e) {
      // OK
    }
  }

  public static void reportPhysicalMemory() {
    com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
        java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    long physicalMemorySize = os.getTotalPhysicalMemorySize();
    System.out.println("************ SYSTEM Physical Memory:"  + physicalMemorySize);

    long MB = 1024 * 1024;
    Runtime runtime = Runtime.getRuntime();
    long free = runtime.freeMemory()/MB;
    long total = runtime.totalMemory()/MB;
    System.out.println("************ total memory:" + total + " free memory:" + free);
  }

  public ZkServer getZKServer() {
    return _zkServer;
  }

  public HelixZkClient getZKClient() {
    return _gZkClient;
  }

  public ClusterSetup getClusterSetupTool() {
    return _gSetupTool;
  }

  public BaseDataAccessor<ZNRecord> getBaseAccessor() {
    return _baseAccessor;
  }

  public MBeanServerConnection getServer() {
    return _server;
  }

  public Map<String, Map<String, HelixZkClient>> getLiveInstanceOwners() {
    return _liveInstanceOwners;
  }

  /**
   * Starts an additional in-memory ZooKeeper for testing.
   * @param i index to be added to the ZK port to avoid conflicts
   * @throws Exception
   */
  private static synchronized void startZooKeeper(int i) {
    String zkAddress = ZK_PREFIX + (ZK_START_PORT + i);
    _zkServerMap.computeIfAbsent(zkAddress, TestHelper::startZkServer);
    _helixZkClientMap.computeIfAbsent(zkAddress, TestExecutionRuntime::createZkClient);
    _clusterSetupMap.computeIfAbsent(zkAddress, key -> new ClusterSetup(_helixZkClientMap.get(key)));
    _baseDataAccessorMap.computeIfAbsent(zkAddress, key -> new ZkBaseDataAccessor(_helixZkClientMap.get(key)));
  }

  private static HelixZkClient createZkClient(String zkAddress) {
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer());
    return DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress), clientConfig);
  }

}