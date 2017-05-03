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
import java.util.Hashtable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.log4j.Logger;

public class MBeanRegistrar {
  private static Logger LOG = Logger.getLogger(MBeanRegistrar.class);

  public static final String DUPLICATE = "Duplicate";
  public static final int MAX_NUM_DUPLICATED_MONITORS = 1000;

  private static MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  /**
   * This method registers an object with specified domain and properties.
   * If the same ObjectName is already registered, this method tags a "Duplicate" property
   * with an incremental number and tries to register the object again until the object is
   * successfully registered.
   */
  public static ObjectName register(Object object, String domain, String... keyValuePairs)
      throws JMException {
    int num = 0;
    while (num < MAX_NUM_DUPLICATED_MONITORS) {
      ObjectName newObjectName = buildObjectName(num, domain, keyValuePairs);
      num++;
      try {
        _beanServer.registerMBean(object, newObjectName);
      } catch (InstanceAlreadyExistsException e) {
        continue;
      } catch (JMException e) {
        LOG.error(String.format("Error in registering: %s",
            buildObjectName(domain, keyValuePairs).getCanonicalName()), e);
        return null;
      }
      return newObjectName;
    }
    LOG.error(String.format(
        "There're already %d %s, no more will be registered.", MAX_NUM_DUPLICATED_MONITORS,
        buildObjectName(domain, keyValuePairs).getCanonicalName()));
    return null;
  }

  public static void unregister(ObjectName objectName) {
    if (objectName != null && _beanServer.isRegistered(objectName)) {
      try {
        _beanServer.unregisterMBean(objectName);
      } catch (JMException e) {
        LOG.warn("Error in un-registering: " + objectName.getCanonicalName(), e);
      }
    }
  }

  public static ObjectName buildObjectName(String domain, String... keyValuePairs)
      throws MalformedObjectNameException{
    return buildObjectName(0, domain, keyValuePairs);
  }

  public static ObjectName buildObjectName(int num, String domain, String... keyValuePairs)
      throws MalformedObjectNameException {

    if (keyValuePairs.length < 2 || keyValuePairs.length % 2 != 0) {
      throw new IllegalArgumentException("key-value pairs for ObjectName must contain even "
          + "number of String and at least 2 String");
    }

    Hashtable<String, String> table = new Hashtable<>();
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      table.put(keyValuePairs[i], keyValuePairs[i + 1]);
    }
    if (num > 0) {
      table.put(DUPLICATE, String.valueOf(num));
    }
    return new ObjectName(domain, table);
  }
}
