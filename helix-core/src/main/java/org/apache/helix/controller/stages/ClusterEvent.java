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
  private final ClusterEventType _eventType;
  private final Map<String, Object> _eventAttributeMap;
  private long _creationTime;
  private String _clusterName;

  @Deprecated
  public ClusterEvent(ClusterEventType eventType) {
    _eventType = eventType;
    _eventAttributeMap = new HashMap<>();
    _creationTime = System.currentTimeMillis();
  }

  public ClusterEvent(String clusterName, ClusterEventType eventType) {
    _clusterName = clusterName;
    _eventType = eventType;
    _eventAttributeMap = new HashMap<>();
    _creationTime = System.currentTimeMillis();
  }

  public void addAttribute(String attrName, Object attrValue) {
    if (logger.isTraceEnabled()) {
      logger.trace("Adding attribute:" + attrName);
      logger.trace(" attribute value:" + attrValue);
    }

    _eventAttributeMap.put(attrName, attrValue);
  }

  public ClusterEventType getEventType() { return _eventType; }

  public long getCreationTime() {
    return _creationTime;
  }

  public void setCreationTime(long creationTime) {
    _creationTime = creationTime;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public void setClusterName(String clusterName) {
    _clusterName = clusterName;
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
    sb.append("name:" + _eventType.name()).append("\n");
    for (String key : _eventAttributeMap.keySet()) {
      sb.append(key).append(":").append(_eventAttributeMap.get(key)).append("\n");
    }
    return sb.toString();
  }
}
