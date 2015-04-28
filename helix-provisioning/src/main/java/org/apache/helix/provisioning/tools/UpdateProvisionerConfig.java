package org.apache.helix.provisioning.tools;

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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixConnection;
import org.apache.helix.api.Resource;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.provisioning.yarn.YarnProvisionerConfig;
import org.apache.log4j.Logger;

/**
 * Update the provisioner config
 */
public class UpdateProvisionerConfig {
  private static Logger LOG = Logger.getLogger(UpdateProvisionerConfig.class);
  private static String updateContainerCount = "updateContainerCount";
  private HelixConnection _connection;

  public UpdateProvisionerConfig(String zkAddress) {
    _connection = new ZkHelixConnection(zkAddress);
    _connection.connect();
  }

  public void setNumContainers(String appName, String serviceName, int numContainers) {
    ResourceId resourceId = ResourceId.from(serviceName);

    ClusterAccessor clusterAccessor = _connection.createClusterAccessor(ClusterId.from(appName));
    Resource resource = clusterAccessor.readResource(resourceId);
    LOG.info("Current provisioner config:" + resource.getProvisionerConfig());

    ResourceConfig.Delta delta = new ResourceConfig.Delta(resourceId);
    YarnProvisionerConfig config = new YarnProvisionerConfig(resourceId);
    config.setNumContainers(numContainers);
    delta.setProvisionerConfig(config);
    delta.setIdealState(resource.getIdealState());
    ResourceConfig updatedResourceConfig = clusterAccessor.updateResource(resourceId, delta);
    LOG.info("Update provisioner config:" + updatedResourceConfig.getProvisionerConfig());

  }

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws ParseException {
    Option zkServerOption =
        OptionBuilder.withLongOpt("zookeeperAddress").withDescription("Provide zookeeper address")
            .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("zookeeperAddress(Required)");

    OptionGroup group = new OptionGroup();
    group.setRequired(true);

    // update container count per service
    Option updateContainerCountOption =
        OptionBuilder.withLongOpt(updateContainerCount)
            .withDescription("appName serviceName numContainers").create();
    updateContainerCountOption.setArgs(3);
    updateContainerCountOption.setRequired(false);
    updateContainerCountOption.setArgName("appName serviceName numContainers");

    group.addOption(updateContainerCountOption);

    Options options = new Options();
    options.addOption(zkServerOption);
    options.addOptionGroup(group);
    CommandLine cliParser = new GnuParser().parse(options, args);

    String zkAddress = cliParser.getOptionValue("zookeeperAddress");
    UpdateProvisionerConfig updater = new UpdateProvisionerConfig(zkAddress);

    if (cliParser.hasOption(updateContainerCount)) {
      String appName = cliParser.getOptionValues(updateContainerCount)[0];
      String serviceName = cliParser.getOptionValues(updateContainerCount)[1];
      int numContainers = Integer.parseInt(cliParser.getOptionValues(updateContainerCount)[2]);
      updater.setNumContainers(appName, serviceName, numContainers);
    }

  }
}
