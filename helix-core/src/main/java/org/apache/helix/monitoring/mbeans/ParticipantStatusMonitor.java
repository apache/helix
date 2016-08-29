package org.apache.helix.monitoring.mbeans;

import java.lang.management.ManagementFactory;

import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;


public class ParticipantStatusMonitor implements ParticipantStatusMonitorMBean {
  private static final Logger LOG = Logger.getLogger(ParticipantStatusMonitor.class);
  private static final String PARTICIPANT_STATUS_KEY = "ParticipantStatus";
  private static final String PARTICIPANT_KEY = "ParticipantName";
  private final MBeanServer _beanServer;
  private final String _participantName;

  private long _receivedMessages = 0;

  public ParticipantStatusMonitor(String participantName) {
    _participantName = participantName;
    _beanServer = ManagementFactory.getPlatformMBeanServer();

    try {
      LOG.info("Register MBean for participant: " + participantName);
      _beanServer.registerMBean(this, getObjectName(getParticipantBeanName()));
    } catch (Exception e) {
      LOG.error("Could not register MBean for : " + participantName, e);
    }
  }

  @Override
  public long getReceivedMessages() {
    return _receivedMessages;
  }

  @Override
  public String getSensorName() {
    return PARTICIPANT_STATUS_KEY + "." + _participantName;
  }

  public ObjectName getObjectName(String name) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s: %s", PARTICIPANT_STATUS_KEY, name));
  }

  private String getParticipantBeanName() {
    return String.format("%s=%s", PARTICIPANT_KEY, _participantName);
  }

  public void incrementReceivedMessages(int count) {
    _receivedMessages+=count;
  }
}
