package ${package}.participant;

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

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixParticipant;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ${name}Participant {
  private static final Logger LOG = LoggerFactory.getLogger(${name}Participant.class);

  private final String zkConnectString;
  private final ClusterId clusterId;
  private final ParticipantId participantId;

  private HelixConnection connection;
  private HelixParticipant participant;

  /**
   * A Participant in the cluster.
   *
   * <p>
   *   http://helix.apache.org/0.7.1-docs/tutorial_participant.html
   * </p>
   *
   * @param zkConnectString
   *  The ZooKeeper cluster for Helix.
   * @param clusterId
   *  The cluster name.
   * @param participantId
   *  This participant id.
   */
  public ${name}Participant(String zkConnectString, ClusterId clusterId, ParticipantId participantId) {
    this.zkConnectString = zkConnectString;
    this.clusterId = clusterId;
    this.participantId = participantId;
  }

  public void start() {
    LOG.info("Connecting to {}", zkConnectString);
    connection = new ZkHelixConnection(zkConnectString);
    connection.connect();

    participant = connection.createParticipant(clusterId, participantId);
    participant.getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.OnlineOffline, new ${name}StateTransitionHandlerFactory());

    LOG.info("Starting participant {} :: {}", clusterId, participantId);
    participant.start();
  }

  public void stop() {
    LOG.info("Stopping participant {} :: {}", clusterId, participantId);
    participant.stop();

    LOG.info("Disconnecting from cluster {}", zkConnectString);
    connection.disconnect();
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("usage: zkConnectString clusterId participantId");
      System.exit(1);
    }

    // Parse args
    String zkConnectString = args[0];
    ClusterId clusterId = ClusterId.from(args[1]);
    ParticipantId participantId = ParticipantId.from(args[2]);

    final ${name}Participant participant = new ${name}Participant(zkConnectString, clusterId, participantId);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        participant.stop();
      }
    });

    participant.start();

    // Block
    final CountDownLatch latch = new CountDownLatch(1);
    latch.await();
  }
}
