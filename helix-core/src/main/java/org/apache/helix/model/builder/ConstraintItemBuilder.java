package org.apache.helix.model.builder;

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

import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintValue;
import org.apache.helix.model.ConstraintItem;
import org.apache.log4j.Logger;

public class ConstraintItemBuilder {

  private static Logger LOG = Logger.getLogger(ConstraintItemBuilder.class);

  // attributes e.g. {STATE:MASTER, RESOURCE:TestDB, INSTANCE:localhost_12918}
  final Map<ConstraintAttribute, String> _attributes = new TreeMap<ConstraintAttribute, String>();
  String _constraintValue = null;

  /**
   * add an attribute to constraint-item, overwrite if already exists
   * @param attribute
   * @param value
   */
  public ConstraintItemBuilder addConstraintAttribute(String attribute, String value) {
    // make sure constraint attribute is valid
    try {
      ConstraintAttribute attr = ConstraintAttribute.valueOf(attribute.toUpperCase());

      if (attr == ConstraintAttribute.CONSTRAINT_VALUE) {
        // make sure constraint-value is valid
        try {
          ConstraintValue.valueOf(value);
          if (_constraintValue == null) {
            LOG.info("overwrite existing constraint-value. old-value: " + _constraintValue
                + ", new-value: " + value);
          }
          _constraintValue = value;
        } catch (IllegalArgumentException e) {
          try {
            Integer.parseInt(value);
            if (_constraintValue == null) {
              LOG.info("overwrite existing constraint-value. old-value: " + _constraintValue
                  + ", new-value: " + value);
            }
            _constraintValue = value;
          } catch (NumberFormatException ne) {
            LOG.error("fail to add constraint attribute. Invalid constraintValue. " + attribute
                + ": " + attribute + ", value: " + value);
          }
        }
      } else {
        if (_attributes.containsKey(attr)) {
          LOG.info("overwrite existing constraint attribute. attribute: " + attribute
              + ", old-value: " + _attributes.get(attr) + ", new-value: " + value);
        }
        _attributes.put(attr, value);
      }
    } catch (IllegalArgumentException e) {
      LOG.error("fail to add constraint attribute. Invalid attribute type. attribute: " + attribute
          + ", value: " + value);
    }

    return this;
  }

  public ConstraintItemBuilder addConstraintAttributes(Map<String, String> attributes) {
    for (String attr : attributes.keySet()) {
      addConstraintAttribute(attr, attributes.get(attr));
    }
    return this;
  }

  public Map<ConstraintAttribute, String> getAttributes() {
    return _attributes;
  }

  public String getConstraintValue() {
    return _constraintValue;
  }

  public ConstraintItem build() {
    // TODO: check if constraint-item is valid
    return new ConstraintItem(_attributes, _constraintValue);
  }
}
