package org.apache.helix.provisioning.yarn.example;

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

import java.io.File;
import java.util.Collection;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRole;
import org.apache.helix.InstanceType;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ContainerConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.provisioning.ApplicationSpec;
import org.apache.helix.provisioning.ApplicationSpecFactory;
import org.apache.helix.provisioning.HelixYarnUtil;
import org.apache.helix.provisioning.TaskConfig;
import org.apache.helix.provisioning.yarn.AppLauncher;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.Workflow;

public class JobRunnerMain {
  public static void main(String[] args) throws Exception {
    Options opts = new Options();
    opts.addOption(new Option("app_spec_provider", true,
        "Application Spec Factory Class that will parse the app_config_spec file"));
    opts.addOption(new Option("app_config_spec", true,
        "YAML config file that provides the app specifications"));
    CommandLine cliParser = new GnuParser().parse(opts, args);
    String appSpecFactoryClass = cliParser.getOptionValue("app_spec_provider");
    String yamlConfigFileName = cliParser.getOptionValue("app_config_spec");

    ApplicationSpecFactory applicationSpecFactory =
        HelixYarnUtil.createInstance(appSpecFactoryClass);
    File yamlConfigFile = new File(yamlConfigFileName);
    if (!yamlConfigFile.exists()) {
      throw new IllegalArgumentException("YAML app_config_spec file: '" + yamlConfigFileName
          + "' does not exist");
    }
    final AppLauncher launcher = new AppLauncher(applicationSpecFactory, yamlConfigFile);
    launcher.launch();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        launcher.cleanup();
      }
    }));

    final ApplicationSpec appSpec = launcher.getApplicationSpec();

    // Repeatedly print status
    final HelixConnection connection = launcher.pollForConnection();
    final ClusterId clusterId = ClusterId.from(appSpec.getAppName());
    // TODO: this is a hack -- TaskDriver should accept a connection instead of a manager
    HelixManager manager = new ZKHelixManager(new HelixRole() {
      @Override
      public HelixConnection getConnection() {
        return connection;
      }

      @Override
      public ClusterId getClusterId() {
        return clusterId;
      }

      @Override
      public Id getId() {
        return null;
      }

      @Override
      public InstanceType getType() {
        return InstanceType.ADMINISTRATOR;
      }

      @Override
      public ClusterMessagingService getMessagingService() {
        return null;
      }

      @Override
      public HelixDataAccessor getAccessor() {
        return null;
      }
    });

    // Get all submitted jobs
    String workflow = null;
    List<TaskConfig> taskConfigs = appSpec.getTaskConfigs();
    if (taskConfigs != null) {
      for (TaskConfig taskConfig : taskConfigs) {
        String yamlFile = taskConfig.getValue("yamlFile");
        if (yamlFile != null) {
          Workflow flow = Workflow.parse(new File(yamlFile));
          workflow = flow.getName();
        }
      }
    }

    // Repeatedly poll for status
    if (workflow != null) {
      ClusterAccessor accessor = connection.createClusterAccessor(clusterId);
      TaskDriver driver = new TaskDriver(manager);
      while (true) {
        System.out.println("CONTAINER STATUS");
        System.out.println("----------------");
        Cluster cluster = accessor.readCluster();
        Collection<Participant> participants = cluster.getParticipantMap().values();
        for (Participant participant : participants) {
          ContainerConfig containerConfig = participant.getContainerConfig();
          if (containerConfig != null) {
            System.out.println(participant.getId() + "[" + containerConfig.getId() + "]: "
                + containerConfig.getState());
          }
          if (participant.isAlive()) {
            LiveInstance runningInstance = participant.getLiveInstance();
            System.out.println("\tProcess: " + runningInstance.getProcessId());
          }
        }
        System.out.println("----------------");
        System.out.println("TASK STATUS");
        System.out.println("----------------");
        driver.list(workflow);
        Thread.sleep(5000);
      }
    }
  }
}
