package org.apache.helix.provisioning.yarn;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ContainerParticipant {
  private static final Log LOG = LogFactory.getLog(ContainerParticipant.class);

  public static void main(String[] args) throws InterruptedException {
    LOG.info("Starting participant: "+ Arrays.toString(args));
    Thread.currentThread().join();
  }
}
