package org.apache.helix.provisioning.yarn;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Map;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.helix.HelixController;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.rebalancer.config.FullAutoRebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

/**
 * This will <br/>
 * <ul>
 * <li>start zookeeper automatically</li>
 * <li>create the cluster</li>
 * <li>set up resource(s)</li>
 * <li>start helix controller</li>
 * </ul>
 */
public class HelixYarnApplicationMasterMain {
  public static Logger LOG = Logger.getLogger(HelixYarnApplicationMasterMain.class);

  public static void main(String[] args) throws Exception {
    Map<String, String> env = System.getenv();
    LOG.info("Starting app master with the following environment variables");
    for(String key: env.keySet()){
      LOG.info(key + "\t\t=" + env.get(key));
    }
    int numContainers = 1;

    Options opts;
    opts = new Options();
    opts.addOption("num_containers", true, "Number of containers");
    try {
      CommandLine cliParser = new GnuParser().parse(opts, args);
      numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers"));
    } catch (Exception e) {
      LOG.error("Error parsing input arguments" + Arrays.toString(args), e);
    }

    // START ZOOKEEPER
    String dataDir = "dataDir";
    String logDir = "logDir";
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {

      @Override
      public void createDefaultNameSpace(ZkClient zkClient) {

      }
    };
    FileUtils.deleteDirectory(new File(dataDir));
    FileUtils.deleteDirectory(new File(logDir));

    final ZkServer server = new ZkServer(dataDir, logDir, defaultNameSpace);
    server.start();

    // start
    AppMasterConfig appMasterConfig = new AppMasterConfig();
    String containerIdStr = appMasterConfig.getContainerId();
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();

    String configFile = AppMasterConfig.AppEnvironment.APP_SPEC_FILE.toString();
    ApplicationSpecFactory factory =
        (ApplicationSpecFactory) Class.forName(appMasterConfig.getApplicationSpecFactory())
            .newInstance();

    GenericApplicationMaster genericApplicationMaster = new GenericApplicationMaster(appAttemptID);
    genericApplicationMaster.start();

    YarnProvisioner.applicationMaster = genericApplicationMaster;
    YarnProvisioner.applicationMasterConfig = appMasterConfig;
    YarnProvisioner.applicationSpec = factory.fromYaml(new FileInputStream(configFile));
    String zkAddress = appMasterConfig.getZKAddress();
    String clusterName = appMasterConfig.getAppName();
    
    String resourceName = "HelloWorld";
    // CREATE CLUSTER and setup the resources
    // connect
    ZkHelixConnection connection = new ZkHelixConnection(zkAddress);
    connection.connect();

    // create the cluster
    ClusterId clusterId = ClusterId.from(clusterName);
    ClusterAccessor clusterAccessor = connection.createClusterAccessor(clusterId);
    StateModelDefinition statelessService =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForStatelessService());
    clusterAccessor.createCluster(new ClusterConfig.Builder(clusterId).addStateModelDefinition(
        statelessService).build());

    // add the resource with the local provisioner
    ResourceId resourceId = ResourceId.from(resourceName);
    YarnProvisionerConfig provisionerConfig = new YarnProvisionerConfig(resourceId);
    provisionerConfig.setNumContainers(numContainers);
    RebalancerConfig rebalancerConfig =
        new FullAutoRebalancerConfig.Builder(resourceId).stateModelDefId(
            statelessService.getStateModelDefId()).build();
    clusterAccessor.addResourceToCluster(new ResourceConfig.Builder(ResourceId.from(resourceName))
        .provisionerConfig(provisionerConfig).rebalancerConfig(rebalancerConfig).build());

    // start controller
    ControllerId controllerId = ControllerId.from("controller1");
    HelixController controller = connection.createController(clusterId, controllerId);
    controller.start();

    Thread shutdownhook = new Thread(new Runnable() {
      @Override
      public void run() {
        server.shutdown();
      }
    });
    Runtime.getRuntime().addShutdownHook(shutdownhook);
    Thread.sleep(10000);

  }
}
