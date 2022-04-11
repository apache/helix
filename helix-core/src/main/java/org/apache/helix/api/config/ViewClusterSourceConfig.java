package org.apache.helix.api.config;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.helix.PropertyType;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Represents source physical cluster information for view cluster
 */
public class ViewClusterSourceConfig {

  private static final List<PropertyType> _validPropertyTypes = Collections.unmodifiableList(Arrays
      .asList(new PropertyType[] { PropertyType.INSTANCES, PropertyType.EXTERNALVIEW,
          PropertyType.LIVEINSTANCES
      }));

  private static ObjectMapper _objectMapper = new ObjectMapper();

  @JsonProperty("name")
  private String _name;

  @JsonProperty("zkAddress")
  String _zkAddress;

  @JsonProperty("properties")
  private List<PropertyType> _properties;

  private ViewClusterSourceConfig() {
  }

  public ViewClusterSourceConfig(String name, String zkAddress, List<PropertyType> properties) {
    _name = name;
    _zkAddress = zkAddress;
    _properties = properties;
  }

  public ViewClusterSourceConfig(ViewClusterSourceConfig config) {
    this(config.getName(), config.getZkAddress(), new ArrayList<>(config.getProperties()));
  }

  public void setName(String name) {
    _name = name;
  }

  public void setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
  }

  public void setProperties(List<PropertyType> properties) {
    for (PropertyType p : properties) {
      if (!_validPropertyTypes.contains(p)) {
        throw new IllegalArgumentException(
            String.format("Property %s is not support in ViewCluster yet.", p));
      }
    }
    _properties = properties;
  }

  public String getName() {
    return _name;
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  public List<PropertyType> getProperties() {
    return _properties;
  }

  public String toJson() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }

  public String toString() {
    return String.format("name=%s; zkAddr=%s; properties=%s", _name, _zkAddress, _properties);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof ViewClusterSourceConfig)) {
      return false;
    }
    ViewClusterSourceConfig otherConfig = (ViewClusterSourceConfig) other;

    return _name.equals(otherConfig.getName()) && _zkAddress.equals(otherConfig.getZkAddress())
        && _properties.containsAll(otherConfig.getProperties()) && otherConfig.getProperties()
        .containsAll(_properties);

  }

  public static ViewClusterSourceConfig fromJson(String jsonString) {
    try {
      return _objectMapper.readValue(jsonString, ViewClusterSourceConfig.class);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Invalid Json: %s, Exception: %s", jsonString, e.toString()));
    }
  }
}
