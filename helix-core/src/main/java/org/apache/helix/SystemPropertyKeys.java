package org.apache.helix;

public class SystemPropertyKeys {
  // Task Driver
  public static final String TASK_CONFIG_LIMITATION = "helixTask.configsLimitation";

  // ZKHelixManager
  public static final String CLUSTER_MANAGER_VERSION = "cluster-manager-version.properties";
  // soft constraints weight definitions
  public static final String SOFT_CONSTRAINT_WEIGHTS = "soft-constraint-weight.properties";

  public static final String FLAPPING_TIME_WINDOW = "helixmanager.flappingTimeWindow";

  // max disconnect count during the flapping time window to trigger HelixManager flapping handling
  public static final String MAX_DISCONNECT_THRESHOLD = "helixmanager.maxDisconnectThreshold";

  public static final String ZK_SESSION_TIMEOUT = "zk.session.timeout";

  public static final String ZK_CONNECTION_TIMEOUT = "zk.connection.timeout";

  @Deprecated
  public static final String ZK_REESTABLISHMENT_CONNECTION_TIMEOUT =
      "zk.connectionReEstablishment.timeout";

  public static final String ZK_WAIT_CONNECTED_TIMEOUT = "helixmanager.waitForConnectedTimeout";

  public static final String PARTICIPANT_HEALTH_REPORT_LATENCY =
      "helixmanager.participantHealthReport.reportLatency";

  // Indicate monitoring level of the HelixManager metrics
  public static final String MONITOR_LEVEL = "helixmanager.monitorLevel";

  // CallbackHandler
  public static final String ASYNC_BATCH_MODE_ENABLED = "helix.callbackhandler.isAsyncBatchModeEnabled";

  public static final String LEGACY_ASYNC_BATCH_MODE_ENABLED = "isAsyncBatchModeEnabled";

  // Controller
  public static final String CONTROLLER_MESSAGE_PURGE_DELAY = "helix.controller.stages.MessageGenerationPhase.messagePurgeDelay";
}
