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
import org.apache.helix.api.id.ConstraintId;
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
    STATE_MODEL,
    MESSAGE_TYPE,
    TRANSITION,
    RESOURCE,
    PARTITION,
    INSTANCE,
    CONSTRAINT_VALUE
  }

  /**
   * Possible special values that constraint attributes can take
   */
  public enum ConstraintValue {
    ANY,
    N,
    R
  }

  /**
   * What is being constrained
   */
  public enum ConstraintType {
    STATE_CONSTRAINT,
    MESSAGE_CONSTRAINT
  }

  // constraint-id -> constraint-item
  private final Map<ConstraintId, ConstraintItem> _constraints =
      new HashMap<ConstraintId, ConstraintItem>();

  /**
   * Instantiate constraints as a given type
   * @param type {@link ConstraintType} representing what this constrains
   */
  public ClusterConstraints(ConstraintType type) {
    super(type.toString());
  }

  /**
   * Get the type of constraint this object represents
   * @return constraint type
   */
  public ConstraintType getType() {
    return ConstraintType.valueOf(getId());
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
        addConstraintItem(ConstraintId.from(constraintId), item);
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
  public void addConstraintItem(ConstraintId constraintId, ConstraintItem item) {
    Map<String, String> map = new TreeMap<String, String>();
    for (ConstraintAttribute attr : item.getAttributes().keySet()) {
      map.put(attr.toString(), item.getAttributeValue(attr));
    }
    map.put(ConstraintAttribute.CONSTRAINT_VALUE.toString(), item.getConstraintValue());
    _record.setMapField(constraintId.stringify(), map);
    _constraints.put(constraintId, item);
  }

  /**
   * add the constraint, overwrite existing one if constraint with same constraint-id already exists
   * @param constraintId unique constraint identifier
   * @param item the constraint as a {@link ConstraintItem}
   */
  public void addConstraintItem(String constraintId, ConstraintItem item) {
    addConstraintItem(ConstraintId.from(constraintId), item);
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
  public void removeConstraintItem(ConstraintId constraintId) {
    _constraints.remove(constraintId);
    _record.getMapFields().remove(constraintId.stringify());
  }

  /**
   * remove a constraint-item
   * @param constraintId unique constraint identifier
   */
  public void removeConstraintItem(String constraintId) {
    removeConstraintItem(ConstraintId.from(constraintId));
  }

  /**
   * get a constraint-item
   * @param constraintId unique constraint identifier
   * @return {@link ConstraintItem} or null if not present
   */
  public ConstraintItem getConstraintItem(ConstraintId constraintId) {
    return _constraints.get(constraintId);
  }

  /**
   * get a constraint-item
   * @param constraintId unique constraint identifier
   * @return {@link ConstraintItem} or null if not present
   */
  public ConstraintItem getConstraintItem(String constraintId) {
    return getConstraintItem(ConstraintId.from(constraintId));
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
   * Get all constraint items in this collection of constraints
   * @return map of constraint id to constraint item
   */
  public Map<ConstraintId, ConstraintItem> getConstraintItems() {
    return _constraints;
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
      if (msg.getTypedFromState() != null && msg.getTypedToState() != null) {
        attributes.put(ConstraintAttribute.TRANSITION,
            Transition.from(msg.getTypedFromState(), msg.getTypedToState()).toString());
      }
      if (msg.getPartitionId() != null) {
        attributes.put(ConstraintAttribute.PARTITION, msg.getPartitionId().stringify());
      }
      if (msg.getResourceId() != null) {
        attributes.put(ConstraintAttribute.RESOURCE, msg.getResourceId().stringify());
      }
      if (msg.getTgtName() != null) {
        attributes.put(ConstraintAttribute.INSTANCE, msg.getTgtName());
      }
      if (msg.getStateModelDefId() != null) {
        attributes.put(ConstraintAttribute.STATE_MODEL, msg.getStateModelDefId().stringify());
      }
    }
    return attributes;
  }

  @Override
  public boolean isValid() {
    return true;
  }

}
