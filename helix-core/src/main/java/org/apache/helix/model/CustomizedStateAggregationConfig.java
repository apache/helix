package org.apache.helix.model;

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

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

/**
 * CustomizedStateAggregation configurations
 */
public class CustomizedStateAggregationConfig extends HelixProperty {
  /**
   * Indicate which customized states will be aggregated.
   * NOTE: Do NOT use this field name directly, use its corresponding getter/setter in the
   * CustomizedStateAggregationConfig.
   */
  public enum CustomizedStateAggregationProperty {
    AGGREGATION_ENABLED_STATES,
  }

  /**
   * Instantiate the CustomizedStateAggregationConfig
   * @param cluster
   */
  public CustomizedStateAggregationConfig(String cluster) {
    super(cluster);
  }

  /**
   * Instantiate with a pre-populated record
   * @param record a ZNRecord corresponding to a CustomizedStateAggregationConfig
   */
  public CustomizedStateAggregationConfig(ZNRecord record) {
    super(record);
  }

  /**
   * Instantiate the config using each field individually.
   * Users should use CustomizedStateAggregationConfig.Builder to create
   * CustomizedStateAggregationConfig.
   * @param cluster
   * @param aggregationEnabledStates
   */
  public CustomizedStateAggregationConfig(String cluster, List<String> aggregationEnabledStates) {
    super(cluster);
    _record.setListField(CustomizedStateAggregationProperty.AGGREGATION_ENABLED_STATES.name(),
        aggregationEnabledStates);

  }

  /**
   * Set the AGGREGATION_ENABLED_STATES field.
   * @param aggregationEnabledStates
   */
  public void setAggregationEnabledStates(List<String> aggregationEnabledStates) {
    _record.setListField(CustomizedStateAggregationProperty.AGGREGATION_ENABLED_STATES.name(),
        aggregationEnabledStates);
  }

  /**
   * Get the AGGREGATION_ENABLED_STATES field.
   * @return AGGREGATION_ENABLED_STATES field.
   */
  public List<String> getAggregationEnabledStates() {
    return _record
        .getListField(CustomizedStateAggregationProperty.AGGREGATION_ENABLED_STATES.name());
  }

  public static class Builder {
    private String _clusterName = null;
    private List<String> _aggregationEnabledStates;

    public CustomizedStateAggregationConfig build() {
      return new CustomizedStateAggregationConfig(_clusterName, _aggregationEnabledStates);
    }

    /**
     * Default constructor
     */
    public Builder() {
    }

    /**
     * Constructor with Cluster Name as input
     * @param clusterName
     */
    public Builder(String clusterName) {
      _clusterName = clusterName;
    }

    /**
     * Constructor with CustomizedStateAggregationConfig as input
     * @param customizedStateAggregationConfig
     */
    public Builder(CustomizedStateAggregationConfig customizedStateAggregationConfig) {
      _aggregationEnabledStates = customizedStateAggregationConfig.getAggregationEnabledStates();
    }

    public Builder setClusterName(String v) {
      _clusterName = v;
      return this;
    }

    public Builder setAggregationEnabledStates(List<String> v) {
      _aggregationEnabledStates = v;
      return this;
    }

    public Builder addAggregationEnabledState(String v) {
      if (_aggregationEnabledStates == null) {
        _aggregationEnabledStates = new ArrayList<String>();
      }
      _aggregationEnabledStates.add(v);
      return this;
    }

    public String getClusterName() {
      return _clusterName;
    }

    public List<String> getAggregationEnabledStates() {
      return _aggregationEnabledStates;
    }
  }
}
