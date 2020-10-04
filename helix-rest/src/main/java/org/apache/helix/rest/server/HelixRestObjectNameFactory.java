package org.apache.helix.rest.server;

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

import java.util.Hashtable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.codahale.metrics.jmx.ObjectNameFactory;
import org.apache.helix.HelixException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates an {@link ObjectName} that has "name", "type" and "namespace" properties
 * for metrics registry in Helix rest service.
 *
 * <p>It is recommended to only be used within Helix REST.
 */
class HelixRestObjectNameFactory implements ObjectNameFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HelixRestObjectNameFactory.class);

  private static final String KEY_NAME = "name";
  private static final String KEY_TYPE = "type";
  private static final String KEY_NAMESPACE = "namespace";

  private final String _namespace;

  HelixRestObjectNameFactory(String nameSpace) {
    _namespace = nameSpace;
  }

  public ObjectName createName(String type, String domain, String name) {
    Hashtable<String, String> properties = new Hashtable<>();

    properties.put(KEY_NAME, name);
    properties.put(KEY_TYPE, type);
    properties.put(KEY_NAMESPACE, _namespace);

    try {
      /*
       * The only way we can find out if we need to quote the properties is by
       * checking an ObjectName that we've constructed. Eg. when regex is used in
       * object name, quoting is needed.
       */
      ObjectName objectName = new ObjectName(domain, properties);
      boolean needQuote = false;

      if (objectName.isDomainPattern()) {
        domain = ObjectName.quote(domain);
        needQuote = true;
      }

      if (objectName.isPropertyValuePattern(KEY_NAME)) {
        properties.put(KEY_NAME, ObjectName.quote(name));
        needQuote = true;
      }

      if (objectName.isPropertyValuePattern(KEY_TYPE)) {
        properties.put(KEY_TYPE, ObjectName.quote(type));
        needQuote = true;
      }

      return needQuote ? new ObjectName(domain, properties) : objectName;
    } catch (MalformedObjectNameException e) {
      throw new HelixException(String
          .format("Unable to register metrics: domain=%s, name=%s, namespace=%s", domain, name,
              _namespace), e);
    }
  }
}
