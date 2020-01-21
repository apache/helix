package org.apache.helix;

public class SystemPropertyKeys {
  // Used to compose default values in HelixManagerProperty
  public static final String HELIX_MANAGER_PROPERTIES = "helix-manager.properties";

  public static final String HELIX_MANAGER_VERSION = "clustermanager.version";

  // Used to compose default values in HelixCloudProperty when cloud provider is Azure
  public static final String AZURE_CLOUD_PROPERTIES = "azure-cloud.properties";

  // Task Driver
  public static final String TASK_CONFIG_LIMITATION = "helixTask.configsLimitation";

  // ZKHelixManager
  public static final String CLUSTER_MANAGER_VERSION = "cluster-manager-version.properties";

  public static final String FLAPPING_TIME_WINDOW = "helixmanager.flappingTimeWindow";

  // max disconnect count during the flapping time window to trigger HelixManager flapping handling
  public static final String MAX_DISCONNECT_THRESHOLD = "helixmanager.maxDisconnectThreshold";

  public static final String ZK_SESSION_TIMEOUT = "zk.session.timeout";

  public static final String ZK_CONNECTION_TIMEOUT = "zk.connection.timeout";

  @Deprecated
  public static final String ZK_REESTABLISHMENT_CONNECTION_TIMEOUT =
      "zk.connectionReEstablishment.timeout";

  public static final String ZK_WAIT_CONNECTED_TIMEOUT = "helixmanager.waitForConnectedTimeout";

  /**
   * Setting this property to true in system properties enables auto compression in ZK serializer.
   * The data will be automatically compressed by
   * {@link org.apache.helix.util.GZipCompressionUtil} when being written to Zookeeper
   * if size of serialized data exceeds the write size limit, which by default is 1 MB or could be
   * set by {@value ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES}.
   * <p>
   * The default value is "true" (enabled).
   */
  public static final String ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED =
      "zk.serializer.znrecord.auto-compress.enabled";

  /**
   * This is property that defines the maximum write size in bytes for ZKRecord's two serializers
   * before serialized data is ready to be written to ZK. This property applies to
   * 1. {@link org.apache.helix.manager.zk.ZNRecordSerializer}
   * 2. {@link org.apache.helix.manager.zk.ZNRecordStreamingSerializer}.
   * <p>
   * If the size of serialized data (no matter whether it is compressed or not) exceeds this
   * configured limit, the data will NOT be written to Zookeeper.
   * <p>
   * Default value is 1 MB. If the configured limit is less than or equal to 0 byte,
   * the default value will be used.
   */
  public static final String ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES =
      "zk.serializer.znrecord.write.size.limit.bytes";

  public static final String PARTICIPANT_HEALTH_REPORT_LATENCY =
      "helixmanager.participantHealthReport.reportLatency";

  // Indicate monitoring level of the HelixManager metrics
  public static final String MONITOR_LEVEL = "helixmanager.monitorLevel";

  // CallbackHandler
  public static final String ASYNC_BATCH_MODE_ENABLED = "helix.callbackhandler.isAsyncBatchModeEnabled";

  public static final String LEGACY_ASYNC_BATCH_MODE_ENABLED = "isAsyncBatchModeEnabled";

  // Controller
  public static final String CONTROLLER_MESSAGE_PURGE_DELAY = "helix.controller.stages.MessageGenerationPhase.messagePurgeDelay";

  // MBean monitor for helix.
  public static final String HELIX_MONITOR_TIME_WINDOW_LENGTH_MS = "helix.monitor.slidingTimeWindow.ms";
}
