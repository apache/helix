package org.apache.helix.tools.commandtools;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CurrentStateCleanUp {
  private static final Logger LOG = LoggerFactory.getLogger(CurrentStateCleanUp.class);

  public static final String zkServer = "zkSvr";
  public static final String cluster = "cluster";
  public static final String instances = "instances";
  public static final String help = "help";

  private static Options parseCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info")
            .create();

    Option zkServerOption =
        OptionBuilder.withLongOpt(zkServer).withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option clusterOption =
        OptionBuilder.withLongOpt(cluster).withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option instancesOption = OptionBuilder.withLongOpt(instances)
        .withDescription("Provide instance names, separated by ','").create();
    instancesOption.setArgs(1);
    instancesOption.setRequired(true);
    instancesOption.setArgName("Instance names (Required)");

    OptionGroup optionGroup = new OptionGroup();
    optionGroup.addOption(zkServerOption);

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(clusterOption);
    options.addOption(instancesOption);

    options.addOptionGroup(optionGroup);

    return options;
  }

  public static void cleanupCurrentStatesForCluster(String zkConnectString, String clusterName,
      List<String> instanceNames) throws Exception {
    HelixManager manager = HelixManagerFactory
        .getZKHelixManager(clusterName, "Administorator", InstanceType.ADMINISTRATOR,
            zkConnectString);
    manager.connect();
    try {
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      List<LiveInstance> liveInstances =
          accessor.getChildValues(accessor.keyBuilder().liveInstances());
      Map<String, LiveInstance> liveInstanceMap = new HashMap<>();
      for (LiveInstance liveInstance : liveInstances) {
        liveInstanceMap.put(liveInstance.getInstanceName(), liveInstance);
      }

      DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
        @Override
        public ZNRecord update(ZNRecord currentData) {
          Set<String> partitionToRemove = new HashSet<>();
          for (String partition : currentData.getMapFields().keySet()) {
            if (currentData.getMapField(partition).get("CURRENT_STATE")
                .equals(HelixDefinedState.DROPPED.name())) {
              partitionToRemove.add(partition);
            }
          }
          currentData.getMapFields().keySet().removeAll(partitionToRemove);
          return currentData;
        }
      };

      for (String instanceName : instanceNames) {
        if (liveInstanceMap.containsKey(instanceName)) {
          LOG.info(
              String.format("Processing cleaning current state for instance: %s", instanceName));
          List<String> currentStateNames = accessor.getChildNames(accessor.keyBuilder()
              .currentStates(instanceName, liveInstanceMap.get(instanceName).getSessionId()));
          for (String currentStateName : currentStateNames) {
            PropertyKey key = accessor.keyBuilder()
                .currentState(instanceName, liveInstanceMap.get(instanceName).getSessionId(),
                    currentStateName);
            accessor.getBaseDataAccessor().update(key.getPath(), updater, AccessOption.PERSISTENT);
            CurrentState currentState = accessor.getProperty(key);
            if (currentState.getPartitionStateMap().size() == 0) {
              accessor.getBaseDataAccessor().remove(key.getPath(), AccessOption.PERSISTENT);
              LOG.info(String
                  .format("Remove current state for instance: %s, resource %s", instanceName,
                      currentStateName));
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      manager.disconnect();
    }
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + CurrentStateCleanUp.class.getName(), cliOptions);
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = ToolsUtil.processCommandLineArgs(args, parseCommandLineOptions());
    String zkConnectString = cmd.getOptionValue(zkServer);
    String clusterName = cmd.getOptionValue(cluster);
    String instance = cmd.getOptionValue(instances);
    List<String> instanceNames = Arrays.asList(instance.split(","));

    LOG.info(String
        .format("Starting cleaning current state with ZK: %s, cluster: %s", zkConnectString,
            clusterName));

    cleanupCurrentStatesForCluster(zkConnectString, clusterName, instanceNames);
  }
}
