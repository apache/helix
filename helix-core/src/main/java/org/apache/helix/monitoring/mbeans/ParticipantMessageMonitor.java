package org.apache.helix.monitoring.mbeans;

public class ParticipantMessageMonitor implements ParticipantMessageMonitorMBean {

  private static final String PARTICIPANT_KEY = "ParticipantName";
  private static final String PARTICIPANT_STATUS_KEY = "ParticipantMessageStatus";
  private final String _participantName;
  private long _receivedMessages = 0;

  public ParticipantMessageMonitor(String participantName) {
    _participantName = participantName;
  }

  public String getParticipantBeanName() {
    return String.format("%s=%s", PARTICIPANT_KEY, _participantName);
  }

  public void incrementReceivedMessages(int count) {
    _receivedMessages+=count;
  }

  @Override
  public long getReceivedMessages() {
    return _receivedMessages;
  }

  @Override
  public String getSensorName() {
    return PARTICIPANT_STATUS_KEY + "." + "_participantName";
  }

}