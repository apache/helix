package org.apache.helix.controller.stages;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ClusterEvent {
  private static final Logger logger = Logger.getLogger(ClusterEvent.class.getName());
  private final String _eventName;
  private final Map<String, Object> _eventAttributeMap;

  public ClusterEvent(String name) {
    _eventName = name;
    _eventAttributeMap = new HashMap<String, Object>();
  }

  public void addAttribute(String attrName, Object attrValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("Adding attribute:" + attrName);
      logger.trace(" attribute value:" + attrValue);
    }

    _eventAttributeMap.put(attrName, attrValue);
  }

  public String getName() {
    return _eventName;
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T getAttribute(String attrName) {
    Object ret = _eventAttributeMap.get(attrName);
    if (ret != null) {
      return (T) ret;
    }
    return null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("name:" + _eventName).append("\n");
    for (String key : _eventAttributeMap.keySet()) {
      sb.append(key).append(":").append(_eventAttributeMap.get(key)).append("\n");
    }
    return sb.toString();
  }
}
