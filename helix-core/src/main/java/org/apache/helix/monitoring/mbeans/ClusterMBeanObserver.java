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

import java.io.IOException;
import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.relation.MBeanServerNotificationFilter;

import org.apache.log4j.Logger;

/*
 * TODO: this class should be in espresso common, as the only usage of it is
 * to create ingraph adaptors
 * **/
public abstract class ClusterMBeanObserver implements NotificationListener {
  protected final String _domain;
  protected MBeanServerConnection _server;
  private static final Logger _logger = Logger.getLogger(ClusterMBeanObserver.class);

  public ClusterMBeanObserver(String domain) throws InstanceNotFoundException, IOException,
      MalformedObjectNameException, NullPointerException {
    // Get a reference to the target MBeanServer
    _domain = domain;
    _server = ManagementFactory.getPlatformMBeanServer();
    MBeanServerNotificationFilter filter = new MBeanServerNotificationFilter();
    filter.enableAllObjectNames();
    _server.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this, filter, null);
  }

  public void handleNotification(Notification notification, Object handback) {
    MBeanServerNotification mbs = (MBeanServerNotification) notification;
    if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(mbs.getType())) {
      if (mbs.getMBeanName().getDomain().equalsIgnoreCase(_domain)) {
        _logger.info("MBean Registered, name :" + mbs.getMBeanName());
        onMBeanRegistered(_server, mbs);
      }
    } else if (MBeanServerNotification.UNREGISTRATION_NOTIFICATION.equals(mbs.getType())) {
      if (mbs.getMBeanName().getDomain().equalsIgnoreCase(_domain)) {
        _logger.info("MBean Unregistered, name :" + mbs.getMBeanName());
        onMBeanUnRegistered(_server, mbs);
      }
    }
  }

  public void disconnect() {
    MBeanServerNotificationFilter filter = new MBeanServerNotificationFilter();
    try {
      _server.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this);
    } catch (Exception e) {
      _logger.error("", e);
    }
  }

  public abstract void onMBeanRegistered(MBeanServerConnection server,
      MBeanServerNotification mbsNotification);

  public abstract void onMBeanUnRegistered(MBeanServerConnection server,
      MBeanServerNotification mbsNotification);

}
