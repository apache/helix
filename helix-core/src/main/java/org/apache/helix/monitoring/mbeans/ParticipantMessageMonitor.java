package org.apache.helix.monitoring.mbeans;

public class ParticipantMessageMonitor implements ParticipantMessageMonitorMBean {

  /**
   * The current processed state of the message
   */
  public enum ProcessedMessageState {
    DISCARDED,
    FAILED,
    COMPLETED
  }

  private static final String PARTICIPANT_KEY = "ParticipantName";
  private static final String PARTICIPANT_STATUS_KEY = "ParticipantMessageStatus";
  private final String _participantName;
  private long _receivedMessages = 0;
  private long _discardedMessages = 0;
  private long _completedMessages = 0;
  private long _failedMessages = 0;
  private long _pendingMessages = 0;

  public ParticipantMessageMonitor(String participantName) {
    _participantName = participantName;
  }

  public String getParticipantBeanName() {
    return String.format("%s=%s", PARTICIPANT_KEY, _participantName);
  }

  public void incrementReceivedMessages(int count) {
    _receivedMessages += count;
  }

  public void incrementDiscardedMessages(int count) {
    _discardedMessages += count;
  }

  public void incrementCompletedMessages(int count) {
    _completedMessages += count;
  }

  public void incrementFailedMessages(int count) {
    _failedMessages += count;
  }

  public void incrementPendingMessages(int count) {
    _pendingMessages += count;
  }

  public void decrementPendingMessages(int count) {
    _pendingMessages -= count;
  }

  @Override
  public long getReceivedMessages() {
    return _receivedMessages;
  }

  @Override
  public long getDiscardedMessages() {
    return _discardedMessages;
  }

  @Override
  public long getCompletedMessages() {
    return _completedMessages;
  }

  @Override
  public long getFailedMessages() {
    return _failedMessages;
  }

  @Override
  public long getPendingMessages() {
    return _pendingMessages;
  }

  @Override
  public String getSensorName() {
    return PARTICIPANT_STATUS_KEY + "." + "_participantName";
  }

}