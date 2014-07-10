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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.helix.HelixConnection;
import org.apache.helix.api.Participant;
import org.apache.helix.api.accessor.ParticipantAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.log4j.Logger;

/**
 *
 *
 */
public class ContainerAdmin {

  private static Logger LOG = Logger.getLogger(ContainerAdmin.class);
  private static String stopContainer = "stopContainer";
  private HelixConnection _connection;

  public ContainerAdmin(String zkAddress) {
    _connection = new ZkHelixConnection(zkAddress);
    _connection.connect();
  }

  public void stopContainer(String appName, String participantName) throws Exception {
    ClusterId clusterId = ClusterId.from(appName);
    ParticipantAccessor participantAccessor = _connection.createParticipantAccessor(clusterId);
    ParticipantId participantId = ParticipantId.from(participantName);
    Participant participant = participantAccessor.readParticipant(participantId);
    if (participant != null && participant.isAlive()) {
      Message message = new Message(MessageType.SHUTDOWN, UUID.randomUUID().toString());
      message.setTgtName(participant.getId().toString());
      message.setTgtSessionId(participant.getRunningInstance().getSessionId());
      message.setMsgId(message.getId());
      Map<MessageId, Message> msgMap = new HashMap<MessageId, Message>();
      msgMap.put(MessageId.from(message.getId()), message);
      participantAccessor.insertMessagesToParticipant(participantId, msgMap);
      do {
        participant = participantAccessor.readParticipant(participantId);
        Thread.sleep(1000);
        LOG.info("Waiting for container:" + participantName + " to shutdown");
      } while (participant != null && participant.isAlive());
    }

  }

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Option zkServerOption =
        OptionBuilder.withLongOpt("zookeeperAddress").withDescription("Provide zookeeper address")
            .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("zookeeperAddress(Required)");

    OptionGroup group = new OptionGroup();
    group.setRequired(true);

    // update container count per service
    Option stopContainerOption =
        OptionBuilder.withLongOpt(stopContainer).withDescription("appName participantName")
            .create();
    stopContainerOption.setArgs(2);
    stopContainerOption.setRequired(false);
    stopContainerOption.setArgName("appName participantName");

    group.addOption(stopContainerOption);

    Options options = new Options();
    options.addOption(zkServerOption);
    options.addOptionGroup(group);
    CommandLine cliParser = new GnuParser().parse(options, args);

    String zkAddress = cliParser.getOptionValue("zookeeperAddress");
    ContainerAdmin admin = new ContainerAdmin(zkAddress);

    if (cliParser.hasOption(stopContainer)) {
      String appName = cliParser.getOptionValues(stopContainer)[0];
      String participantName = cliParser.getOptionValues(stopContainer)[1];
      admin.stopContainer(appName, participantName);
    }
  }
}
