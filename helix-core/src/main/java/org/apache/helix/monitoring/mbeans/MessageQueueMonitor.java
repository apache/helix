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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageQueueMonitor extends DynamicMBeanProvider {
  private static final String MBEAN_DESCRIPTION = "Message Queue Monitor";
  private static final Logger LOG = LoggerFactory.getLogger(MessageQueueMonitor.class);

  private final String _clusterName;
  private final String _instanceName;
  private final MBeanServer _beanServer;
  private SimpleDynamicMetric<Long> _messageQueueBacklog;

  public MessageQueueMonitor(String clusterName, String instanceName) {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    _messageQueueBacklog = new SimpleDynamicMetric("MessageQueueBacklog", 0L);
  }

  /**
   * Set the current backlog size for this instance
   * @param size the message queue size
   */
  public void setMessageQueueBacklog(long size) {
    _messageQueueBacklog.updateValue(size);
  }

  /**
   * Register this bean with the server
   */
  public void init() {
    try {
      register();
    } catch (Exception e) {
      LOG.error("Fail to register MessageQueueMonitor", e);
    }
  }

  /**
   * Remove this bean from the server
   */
  public void reset() {
    _messageQueueBacklog.updateValue(0L);
    try {
      unregister();
    } catch (Exception e) {
      LOG.error("Fail to register MessageQueueMonitor", e);
    }
  }

  @Override
  public String getSensorName() {
    return ClusterStatusMonitor.MESSAGE_QUEUE_STATUS_KEY + "." + _clusterName;
  }

  /**
   * This method registers the dynamic metrics.
   * @return
   * @throws JMException
   */
  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_messageQueueBacklog);
    doRegister(attributeList, MBEAN_DESCRIPTION, getObjectName(getBeanName()));
    return this;
  }

  private String getClusterBeanName() {
    return String.format("%s=%s", ClusterStatusMonitor.CLUSTER_DN_KEY, _clusterName);
  }

  private String getBeanName() {
    return String.format("%s,%s=%s", getClusterBeanName(),
        ClusterStatusMonitor.MESSAGE_QUEUE_DN_KEY, _instanceName);
  }

  public ObjectName getObjectName(String name) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), name));
  }
}
