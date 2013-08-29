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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.log4j.Logger;

/**
 * All of the constraints on a given cluster and its subcomponents, both physical and logical.
 */
public class ClusterConstraints extends HelixProperty {
  private static Logger LOG = Logger.getLogger(ClusterConstraints.class);

  /**
   * Attributes on which constraints operate
   */
  public enum ConstraintAttribute {
    STATE,
    MESSAGE_TYPE,
    TRANSITION,
    RESOURCE,
    INSTANCE,
    CONSTRAINT_VALUE
  }

  /**
   * Possible special values that constraint attributes can take
   */
  public enum ConstraintValue {
    ANY
  }

  /**
   * What is being constrained
   */
  public enum ConstraintType {
    STATE_CONSTRAINT,
    MESSAGE_CONSTRAINT
  }

  // constraint-id -> constraint-item
  private final Map<String, ConstraintItem> _constraints = new HashMap<String, ConstraintItem>();

  /**
   * Instantiate constraints as a given type
   * @param type {@link ConstraintType} representing what this constrains
   */
  public ClusterConstraints(ConstraintType type) {
    super(type.toString());
  }

  /**
   * Instantiate constraints from a pre-populated ZNRecord
   * @param record ZNRecord containing all constraints
   */
  public ClusterConstraints(ZNRecord record) {
    super(record);

    for (String constraintId : _record.getMapFields().keySet()) {
      ConstraintItemBuilder builder = new ConstraintItemBuilder();
      ConstraintItem item =
          builder.addConstraintAttributes(_record.getMapField(constraintId)).build();
      // ignore item with empty attributes or no constraint-value
      if (item.getAttributes().size() > 0 && item.getConstraintValue() != null) {
        addConstraintItem(constraintId, item);
      } else {
        LOG.error("Skip invalid constraint. key: " + constraintId + ", value: "
            + _record.getMapField(constraintId));
      }
    }
  }

  /**
   * add the constraint, overwrite existing one if constraint with same constraint-id already exists
   * @param constraintId unique constraint identifier
   * @param item the constraint as a {@link ConstraintItem}
   */
  public void addConstraintItem(String constraintId, ConstraintItem item) {
    Map<String, String> map = new TreeMap<String, String>();
    for (ConstraintAttribute attr : item.getAttributes().keySet()) {
      map.put(attr.toString(), item.getAttributeValue(attr));
    }
    map.put(ConstraintAttribute.CONSTRAINT_VALUE.toString(), item.getConstraintValue());
    _record.setMapField(constraintId, map);
    _constraints.put(constraintId, item);
  }

  /**
   * Add multiple constraint items.
   * @param items (constraint identifier, {@link ConstrantItem}) pairs
   */
  public void addConstraintItems(Map<String, ConstraintItem> items) {
    for (String constraintId : items.keySet()) {
      addConstraintItem(constraintId, items.get(constraintId));
    }
  }

  /**
   * remove a constraint-item
   * @param constraintId unique constraint identifier
   */
  public void removeConstraintItem(String constraintId) {
    _constraints.remove(constraintId);
    _record.getMapFields().remove(constraintId);
  }

  /**
   * get a constraint-item
   * @param constraintId unique constraint identifier
   * @return {@link ConstraintItem} or null if not present
   */
  public ConstraintItem getConstraintItem(String constraintId) {
    return _constraints.get(constraintId);
  }

  /**
   * return a set of constraints that match the attribute pairs
   * @param attributes (constraint scope, constraint string) pairs
   * @return a set of {@link ConstraintItem}s with matching attributes
   */
  public Set<ConstraintItem> match(Map<ConstraintAttribute, String> attributes) {
    Set<ConstraintItem> matches = new HashSet<ConstraintItem>();
    for (ConstraintItem item : _constraints.values()) {
      if (item.match(attributes)) {
        matches.add(item);
      }
    }
    return matches;
  }

  /**
   * convert a message to constraint attribute pairs
   * @param msg a {@link Message} containing constraint attributes
   * @return constraint attribute scope-value pairs
   */
  public static Map<ConstraintAttribute, String> toConstraintAttributes(Message msg) {
    Map<ConstraintAttribute, String> attributes = new TreeMap<ConstraintAttribute, String>();
    String msgType = msg.getMsgType();
    attributes.put(ConstraintAttribute.MESSAGE_TYPE, msgType);
    if (MessageType.STATE_TRANSITION.toString().equals(msgType)) {
      if (msg.getFromStateString() != null && msg.getToStateString() != null) {
        attributes.put(ConstraintAttribute.TRANSITION, msg.getFromStateString() + "-" + msg.getToStateString());
      }
      if (msg.getResourceName() != null) {
        attributes.put(ConstraintAttribute.RESOURCE, msg.getResourceName());
      }
      if (msg.getTgtName() != null) {
        attributes.put(ConstraintAttribute.INSTANCE, msg.getTgtName());
      }
    }
    return attributes;
  }

  @Override
  public boolean isValid() {
    // TODO Auto-generated method stub
    return true;
  }

}
