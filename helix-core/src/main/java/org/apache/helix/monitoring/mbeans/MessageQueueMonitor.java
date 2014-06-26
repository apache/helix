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

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

public class MessageQueueMonitor implements MessageQueueMonitorMBean {
  private static final Logger LOG = Logger.getLogger(MessageQueueMonitor.class);

  private final String _clusterName;
  private final String _instanceName;
  private final MBeanServer _beanServer;
  private long _messageQueueBacklog;

  public MessageQueueMonitor(String clusterName, String instanceName) {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    _messageQueueBacklog = 0;
  }

  /**
   * Set the current backlog size for this instance
   * @param size the message queue size
   */
  public void setMessageQueueBacklog(long size) {
    _messageQueueBacklog = size;
  }

  @Override
  public long getMessageQueueBacklog() {
    return _messageQueueBacklog;
  }

  /**
   * Register this bean with the server
   */
  public void init() {
    try {
      register(this, getObjectName(getBeanName()));
    } catch (Exception e) {
      LOG.error("Fail to register MessageQueueMonitor", e);
    }
  }

  /**
   * Remove this bean from the server
   */
  public void reset() {
    _messageQueueBacklog = 0;
    try {
      unregister(getObjectName(getBeanName()));
    } catch (Exception e) {
      LOG.error("Fail to register MessageQueueMonitor", e);
    }
  }

  @Override
  public String getSensorName() {
    return ClusterStatusMonitor.MESSAGE_QUEUE_STATUS_KEY + "." + _clusterName;
  }

  private void register(Object bean, ObjectName name) {
    try {
      if (_beanServer.isRegistered(name)) {
        _beanServer.unregisterMBean(name);
      }
    } catch (Exception e) {
      // OK
    }

    try {
      LOG.info("Register MBean: " + name);
      _beanServer.registerMBean(bean, name);
    } catch (Exception e) {
      LOG.warn("Could not register MBean: " + name, e);
    }
  }

  private void unregister(ObjectName name) {
    try {
      if (_beanServer.isRegistered(name)) {
        LOG.info("Unregistering " + name.toString());
        _beanServer.unregisterMBean(name);
      }
    } catch (Exception e) {
      LOG.warn("Could not unregister MBean: " + name, e);
    }
  }

  private String getClusterBeanName() {
    return String.format("%s=%s", ClusterStatusMonitor.CLUSTER_DN_KEY, _clusterName);
  }

  private String getBeanName() {
    return String.format("%s,%s=%s", getClusterBeanName(),
        ClusterStatusMonitor.MESSAGE_QUEUE_DN_KEY, _instanceName);
  }

  public ObjectName getObjectName(String name) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s: %s", ClusterStatusMonitor.CLUSTER_STATUS_KEY, name));
  }
}
