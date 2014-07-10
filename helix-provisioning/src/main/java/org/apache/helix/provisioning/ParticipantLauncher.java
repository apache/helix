package org.apache.helix.provisioning;

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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.helix.HelixConnection;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.participant.AbstractParticipantService;
import org.apache.log4j.Logger;

/**
 * Main class that invokes the Participant Api
 */
public class ParticipantLauncher {
  private static Logger LOG = Logger.getLogger(ParticipantLauncher.class);

  public static void main(String[] args) {

    System.out.println("Starting Helix Participant: " + Arrays.toString(args));
    Options opts;
    opts = new Options();
    opts.addOption("cluster", true, "Cluster name, default app name");
    opts.addOption("participantId", true, "Participant Id");
    opts.addOption("zkAddress", true, "Zookeeper address");
    opts.addOption("participantClass", true, "Participant service class");
    try {
      CommandLine cliParser = new GnuParser().parse(opts, args);
      String zkAddress = cliParser.getOptionValue("zkAddress");
      final HelixConnection connection = new ZkHelixConnection(zkAddress);
      connection.connect();
      ClusterId clusterId = ClusterId.from(cliParser.getOptionValue("cluster"));
      ParticipantId participantId = ParticipantId.from(cliParser.getOptionValue("participantId"));
      String participantClass = cliParser.getOptionValue("participantClass");
      @SuppressWarnings("unchecked")
      Class<? extends AbstractParticipantService> clazz =
          (Class<? extends AbstractParticipantService>) Class.forName(participantClass);
      final AbstractParticipantService containerParticipant =
          clazz.getConstructor(HelixConnection.class, ClusterId.class, ParticipantId.class)
              .newInstance(connection, clusterId, participantId);
      containerParticipant.startAsync();
      containerParticipant.awaitRunning(60, TimeUnit.SECONDS);
      containerParticipant
          .getParticipant()
          .getMessagingService()
          .registerMessageHandlerFactory(MessageType.SHUTDOWN.toString(),
              new ShutdownMessageHandlerFactory(containerParticipant, connection));
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          LOG.info("Received a shutdown signal. Stopping participant");
          containerParticipant.stopAsync();
          containerParticipant.awaitTerminated();
          connection.disconnect();
        }
      }) {

      });
      Thread.currentThread().join();
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Failed to start Helix participant" + e);
      // System.exit(1);
    }
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  public static class ShutdownMessageHandlerFactory implements MessageHandlerFactory {
    private final AbstractParticipantService _service;
    private final HelixConnection _connection;

    public ShutdownMessageHandlerFactory(AbstractParticipantService service,
        HelixConnection connection) {
      _service = service;
      _connection = connection;
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ShutdownMessageHandler(_service, _connection, message, context);
    }

    @Override
    public String getMessageType() {
      return MessageType.SHUTDOWN.toString();
    }

    @Override
    public void reset() {
    }

  }

  public static class ShutdownMessageHandler extends MessageHandler {
    private final AbstractParticipantService _service;
    private final HelixConnection _connection;

    public ShutdownMessageHandler(AbstractParticipantService service, HelixConnection connection,
        Message message, NotificationContext context) {
      super(message, context);
      _service = service;
      _connection = connection;
    }

    @Override
    public HelixTaskResult handleMessage() throws InterruptedException {
      LOG.info("Received a shutdown message. Trying to shut down.");
      _service.stopAsync();
      _service.awaitTerminated();
      _connection.disconnect();
      LOG.info("Shutdown complete. Process exiting gracefully");
      System.exit(0);
      return null;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOG.error("Shutdown message error", e);
    }

  }
}
