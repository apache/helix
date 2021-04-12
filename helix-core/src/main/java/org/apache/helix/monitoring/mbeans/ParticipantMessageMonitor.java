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

import java.util.ArrayList;
import java.util.List;
import javax.management.JMException;

import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

public class ParticipantMessageMonitor extends DynamicMBeanProvider {
  private static final String MBEAN_DESCRIPTION = "Helix Participant Message Monitor";
  private final String _domainName;
  public static final String PARTICIPANT_STATUS_KEY = "ParticipantMessageStatus";

  private final String _participantName;

  private SimpleDynamicMetric<Long> _receivedMessages;
  private SimpleDynamicMetric<Long> _discardedMessages;
  private SimpleDynamicMetric<Long> _completedMessages;
  private SimpleDynamicMetric<Long> _failedMessages;
  private SimpleDynamicMetric<Long> _pendingMessages;

  /**
   * The current processed state of the message
   */
  public enum ProcessedMessageState {
    DISCARDED,
    FAILED,
    COMPLETED
  }

  public ParticipantMessageMonitor(String domainName, String participantName) {
    _domainName = domainName;
    _participantName = participantName;
    _receivedMessages = new SimpleDynamicMetric("ReceivedMessages", 0L);
    _discardedMessages = new SimpleDynamicMetric("DiscardedMessages", 0L);
    _completedMessages = new SimpleDynamicMetric("CompletedMessages", 0L);
    _failedMessages = new SimpleDynamicMetric("FailedMessages", 0L);
    _pendingMessages = new SimpleDynamicMetric("PendingMessages", 0L);
  }

  public void incrementReceivedMessages(long count) {
    incrementSimpleDynamicMetric(_receivedMessages, count);
  }

  public void incrementDiscardedMessages(int count) {
    incrementSimpleDynamicMetric(_discardedMessages, count);
  }

  public void incrementCompletedMessages(int count) {
    incrementSimpleDynamicMetric(_completedMessages, count);
  }

  public void incrementFailedMessages(int count) {
    incrementSimpleDynamicMetric(_failedMessages, count);
  }

  public void incrementPendingMessages(int count) {
    incrementSimpleDynamicMetric(_pendingMessages, count);
  }

  public void decrementPendingMessages(int count) {
    incrementSimpleDynamicMetric(_pendingMessages, -1 * count);
  }

  @Override
  public String getSensorName() {
    return PARTICIPANT_STATUS_KEY;
  }

  /**
   * This method registers the dynamic metrics.
   * @return
   * @throws JMException
   */
  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_receivedMessages);
    attributeList.add(_discardedMessages);
    attributeList.add(_completedMessages);
    attributeList.add(_failedMessages);
    attributeList.add(_pendingMessages);
    doRegister(attributeList, MBEAN_DESCRIPTION, _domainName,
        ParticipantStatusMonitor.PARTICIPANT_KEY, _participantName, "MonitorType",
        ParticipantMessageMonitor.class.getSimpleName());
    return this;
  }
}
