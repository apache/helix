package org.apache.helix.provisioning.yarn;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
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
import org.apache.helix.controller.rebalancer.config.FullAutoRebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.manager.zk.HelixConnectionAdaptor;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.provisioning.ApplicationSpec;
import org.apache.helix.provisioning.ApplicationSpecFactory;
import org.apache.helix.provisioning.HelixYarnUtil;
import org.apache.helix.provisioning.ServiceConfig;
import org.apache.helix.provisioning.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.Workflow;
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
public class AppMasterLauncher {
  public static Logger LOG = Logger.getLogger(AppMasterLauncher.class);

  public static void main(String[] args) throws Exception {
    Map<String, String> env = System.getenv();
    LOG.info("Starting app master with the following environment variables");
    for (String key : env.keySet()) {
      LOG.info(key + "\t\t=" + env.get(key));
    }

    Options opts;
    opts = new Options();
    opts.addOption("num_containers", true, "Number of containers");

    // START ZOOKEEPER
    String dataDir = "dataDir";
    String logDir = "logDir";
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient) {

      }
    };
    try {
      FileUtils.deleteDirectory(new File(dataDir));
      FileUtils.deleteDirectory(new File(logDir));
    } catch (IOException e) {
      LOG.error(e);
    }

    final ZkServer server = new ZkServer(dataDir, logDir, defaultNameSpace);
    server.start();

    // start Generic AppMaster that interacts with Yarn RM
    AppMasterConfig appMasterConfig = new AppMasterConfig();
    String containerIdStr = appMasterConfig.getContainerId();
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();

    String configFile = AppMasterConfig.AppEnvironment.APP_SPEC_FILE.toString();
    String className = appMasterConfig.getApplicationSpecFactory();

    GenericApplicationMaster genericApplicationMaster = new GenericApplicationMaster(appAttemptID);
    try {
      genericApplicationMaster.start();
    } catch (Exception e) {
      LOG.error("Unable to start application master: ", e);
    }
    ApplicationSpecFactory factory = HelixYarnUtil.createInstance(className);

    // TODO: Avoid setting static variable.
    YarnProvisioner.applicationMaster = genericApplicationMaster;
    YarnProvisioner.applicationMasterConfig = appMasterConfig;
    ApplicationSpec applicationSpec = factory.fromYaml(new FileInputStream(configFile));
    YarnProvisioner.applicationSpec = applicationSpec;
    String zkAddress = appMasterConfig.getZKAddress();
    String clusterName = appMasterConfig.getAppName();

    // CREATE CLUSTER and setup the resources
    // connect
    ZkHelixConnection connection = new ZkHelixConnection(zkAddress);
    connection.connect();

    // create the cluster
    ClusterId clusterId = ClusterId.from(clusterName);
    ClusterAccessor clusterAccessor = connection.createClusterAccessor(clusterId);
    StateModelDefinition statelessService =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForStatelessService());
    StateModelDefinition taskStateModel =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForTaskStateModel());
    clusterAccessor.createCluster(new ClusterConfig.Builder(clusterId)
        .addStateModelDefinition(statelessService).addStateModelDefinition(taskStateModel).build());
    for (String service : applicationSpec.getServices()) {
      String resourceName = service;
      // add the resource with the local provisioner
      ResourceId resourceId = ResourceId.from(resourceName);

      ServiceConfig serviceConfig = applicationSpec.getServiceConfig(resourceName);
      serviceConfig.setSimpleField("service_name", service);
      int numContainers = serviceConfig.getIntField("num_containers", 1);

      YarnProvisionerConfig provisionerConfig = new YarnProvisionerConfig(resourceId);
      provisionerConfig.setNumContainers(numContainers);

      FullAutoRebalancerConfig.Builder rebalancerConfigBuilder =
          new FullAutoRebalancerConfig.Builder(resourceId);
      RebalancerConfig rebalancerConfig =
          rebalancerConfigBuilder.stateModelDefId(statelessService.getStateModelDefId())//
              .build();
      ResourceConfig.Builder resourceConfigBuilder =
          new ResourceConfig.Builder(ResourceId.from(resourceName));
      ResourceConfig resourceConfig = resourceConfigBuilder.provisionerConfig(provisionerConfig) //
          .rebalancerConfig(rebalancerConfig) //
          .userConfig(serviceConfig) //
          .build();
      clusterAccessor.addResourceToCluster(resourceConfig);
    }
    // start controller
    ControllerId controllerId = ControllerId.from("controller1");
    HelixController controller = connection.createController(clusterId, controllerId);
    controller.start();

    // Start any pre-specified jobs
    List<TaskConfig> taskConfigs = applicationSpec.getTaskConfigs();
    if (taskConfigs != null) {
      for (TaskConfig taskConfig : taskConfigs) {
        String yamlFile = taskConfig.getValue("yamlFile");
        if (yamlFile != null) {
          File file = new File(yamlFile);
          Workflow workflow = Workflow.parse(file);
          TaskDriver taskDriver = new TaskDriver(new HelixConnectionAdaptor(controller));
          taskDriver.start(workflow);
        }
      }
    }

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
