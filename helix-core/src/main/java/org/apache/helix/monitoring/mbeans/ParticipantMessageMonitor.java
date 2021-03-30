package org.apache.helix.monitoring.mbeans;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

public class ParticipantMessageMonitor implements ParticipantMessageMonitorMBean {
  public static final String PARTICIPANT_KEY = "ParticipantName";
  public static final String PARTICIPANT_STATUS_KEY = "ParticipantMessageStatus";

  /**
   * The current processed state of the message
   */
  public enum ProcessedMessageState {
    DISCARDED,
    FAILED,
    COMPLETED
  }

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
    return PARTICIPANT_STATUS_KEY;
  }

}
