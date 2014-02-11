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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.monitoring.StatCollector;
import org.apache.log4j.Logger;

public class MessageQueueMonitor implements MessageQueueMonitorMBean {
  private static final Logger LOG = Logger.getLogger(MessageQueueMonitor.class);

  private final StatCollector _messageQueueSizeStat;
  private final String _clusterName;
  private final String _instanceName;

  public MessageQueueMonitor(String clusterName, String instanceName) {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _messageQueueSizeStat = new StatCollector();
  }

  public void addMessageQueueSize(long size) {
    _messageQueueSizeStat.addData(size);
  }

  public void reset() {
    _messageQueueSizeStat.reset();
  }

  @Override
  public double getMaxMessageQueueSize() {
    return _messageQueueSizeStat.getMax();
  }

  @Override
  public double getMeanMessageQueueSize() {
    return _messageQueueSizeStat.getMean();
  }

  @Override
  public String getSensorName() {
    return ClusterStatusMonitor.MESSAGE_QUEUE_STATUS_KEY + "." + _clusterName + "." + _instanceName;
  }
}
