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

import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Customized state configurations
 */
public class CustomizedStateConfig extends HelixProperty {

  public static final String CUSTOMIZED_STATE_CONFIG_KW =
      "CustomizedStateConfig";

  /**
   * Indicate which customized states will be aggregated.
   * NOTE: Do NOT use this field name directly, use its corresponding getter/setter in the
   * CustomizedStateConfig.
   */
  public enum CustomizedStateProperty {
    AGGREGATION_ENABLED_TYPES,
  }

  /**
   * Instantiate the CustomizedStateConfig
   */
  public CustomizedStateConfig() {
    super(CUSTOMIZED_STATE_CONFIG_KW);
  }

  /**
   * Instantiate with a pre-populated record
   * @param record a ZNRecord corresponding to a CustomizedStateConfig
   */
  public CustomizedStateConfig(ZNRecord record) {
    super(CUSTOMIZED_STATE_CONFIG_KW);
    _record.setSimpleFields(record.getSimpleFields());
    _record.setListFields(record.getListFields());
    _record.setMapFields(record.getMapFields());
  }

  /**
   * Set the AGGREGATION_ENABLED_TYPES field.
   * @param aggregationEnabledTypes
   */
  public void setAggregationEnabledTypes(List<String> aggregationEnabledTypes) {
    _record.setListField(CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name(),
        aggregationEnabledTypes);
  }

  /**
   * Get the AGGREGATION_ENABLED_TYPES field.
   * @return AGGREGATION_ENABLED_TYPES field.
   */
  public List<String> getAggregationEnabledTypes() {
    return _record
        .getListField(CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name());
  }

  public static class Builder {
    private ZNRecord _record;


    public CustomizedStateConfig build() {
      return new CustomizedStateConfig(_record);
    }

    /**
     * Default constructor
     */
    public Builder() {
      _record = new ZNRecord(CUSTOMIZED_STATE_CONFIG_KW);
    }

    /**
     * Instantiate with a pre-populated record
     * @param record a ZNRecord corresponding to a Customized State Aggregation configuration
     */
    public Builder(ZNRecord record) {
      _record = record;
    }

    /**
     * Constructor with CustomizedStateConfig as input
     * @param customizedStateConfig
     */
    public Builder(CustomizedStateConfig customizedStateConfig) {
      _record = customizedStateConfig.getRecord();
    }

    public Builder setAggregationEnabledTypes(List<String> aggregationEnabledTypes) {
      _record.setListField(CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name(), aggregationEnabledTypes);
      return this;
    }

    public Builder addAggregationEnabledType(String type) {
      if (_record.getListField(
          CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name()) == null) {
        _record.setListField(CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name(), new ArrayList<String>());
      }
      List<String> aggregationEnabledTypes = _record.getListField(CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name());
      aggregationEnabledTypes.add(type);
      _record.setListField(CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name(), aggregationEnabledTypes);
      return this;
    }

    public Builder removeAggregationEnabledType(String type) {
      if (!_record.getListField(CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name())
          .contains(type)) {
        throw new HelixException(
            "Type " + type + " is missing from the CustomizedStateConfig");
      }
      _record.getListField(CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name())
          .remove(type);
      return this;
    }

    public List<String> getAggregationEnabledTypes() {
      return _record
          .getListField(CustomizedStateProperty.AGGREGATION_ENABLED_TYPES.name());
    }
  }
}
