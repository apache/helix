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
import javax.management.InstanceAlreadyExistsException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MBeanRegistrar {
  private static Logger LOG = LoggerFactory.getLogger(MBeanRegistrar.class);

  public static final String DUPLICATE = "Duplicate";
  public static final int MAX_NUM_DUPLICATED_MONITORS = 1000;

  private static MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  /**
   * This method registers an object with specified ObjectName.
   * If the same ObjectName is already registered, this method tags a "Duplicate" property
   * with an incremental number and tries to register the object again until the object is
   * successfully registered.
   */
  public static ObjectName register(Object object, ObjectName objectName) throws JMException {
    int num = 0;
    while (num < MAX_NUM_DUPLICATED_MONITORS) {
      ObjectName newObjectName;
      if (num > 0) {
        newObjectName = new ObjectName(
            String.format("%s,%s=%s", objectName.toString(), DUPLICATE, String.valueOf(num)));
      } else {
        newObjectName = objectName;
      }
      num++;
      try {
        _beanServer.registerMBean(object, newObjectName);
        LOG.info("MBean {} has been registered.", newObjectName.getCanonicalName());
      } catch (InstanceAlreadyExistsException e) {
        continue;
      } catch (JMException e) {
        LOG.error(String.format("Error in registering: %s", objectName.getCanonicalName()), e);
        return null;
      }
      return newObjectName;
    }
    LOG.error(String
        .format("There're already %d %s, no more will be registered.", MAX_NUM_DUPLICATED_MONITORS,
            objectName.getCanonicalName()));
    return null;
  }

  /**
   * This method registers an object with specified domain and properties.
   * If the same ObjectName is already registered, this method tags a "Duplicate" property
   * with an incremental number and tries to register the object again until the object is
   * successfully registered.
   */
  public static ObjectName register(Object object, String domain, String... keyValuePairs)
      throws JMException {
    return register(object, buildObjectName(domain, keyValuePairs));
  }

  public static void unregister(ObjectName objectName) {
    if (objectName != null && _beanServer.isRegistered(objectName)) {
      try {
        _beanServer.unregisterMBean(objectName);
        LOG.info("MBean {} has been un-registered.", objectName.getCanonicalName());
      } catch (JMException e) {
        LOG.warn("Error in un-registering: " + objectName.getCanonicalName(), e);
      }
    }
  }

  public static ObjectName buildObjectName(String domain, String... keyValuePairs)
      throws MalformedObjectNameException {
    if (keyValuePairs.length < 2 || keyValuePairs.length % 2 != 0) {
      throw new IllegalArgumentException("key-value pairs for ObjectName must contain even "
          + "number of String and at least 2 String");
    }

    StringBuilder objectNameStr = new StringBuilder();
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      objectNameStr.append(
          String.format(i == 0 ? "%s=%s" : ",%s=%s", keyValuePairs[i], keyValuePairs[i + 1]));
    }

    return new ObjectName(String.format("%s:%s", domain, objectNameStr.toString()));
  }
}
