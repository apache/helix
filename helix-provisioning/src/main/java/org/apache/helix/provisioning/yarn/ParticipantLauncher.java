package org.apache.helix.provisioning.yarn;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.helix.HelixConnection;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.manager.zk.AbstractParticipantService;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.log4j.Logger;
/**
 * 
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
    opts.addOption("ParticipantClass", true, "ParticipantClass");
    try {
      CommandLine cliParser = new GnuParser().parse(opts, args);
      String zkAddress = cliParser.getOptionValue("zkAddress");
      HelixConnection connection = new ZkHelixConnection(zkAddress);
      connection.connect();
      ClusterId clusterId = ClusterId.from(cliParser.getOptionValue("cluster"));
      ParticipantId participantId = ParticipantId.from(cliParser.getOptionValue("participantId"));
      String participantClass = cliParser.getOptionValue("ParticipantClass");
      @SuppressWarnings("unchecked")
      Class<? extends AbstractParticipantService> clazz =
          (Class<? extends AbstractParticipantService>) Class.forName(participantClass);
      AbstractParticipantService containerParticipant =
          clazz.getConstructor(HelixConnection.class, ClusterId.class, ParticipantId.class)
              .newInstance(connection, clusterId, participantId);
      containerParticipant.startAsync();
      containerParticipant.awaitRunning(60, TimeUnit.SECONDS);
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
}
