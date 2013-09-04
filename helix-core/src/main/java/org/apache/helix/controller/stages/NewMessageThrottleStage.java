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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.Resource;
import org.apache.helix.api.ResourceId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ClusterConstraints.ConstraintValue;
import org.apache.log4j.Logger;

public class NewMessageThrottleStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(NewMessageThrottleStage.class.getName());

  int valueOf(String valueStr) {
    int value = Integer.MAX_VALUE;

    try {
      ConstraintValue valueToken = ConstraintValue.valueOf(valueStr);
      switch (valueToken) {
      case ANY:
        value = Integer.MAX_VALUE;
        break;
      default:
        LOG.error("Invalid constraintValue token:" + valueStr + ". Use default value:"
            + Integer.MAX_VALUE);
        break;
      }
    } catch (Exception e) {
      try {
        value = Integer.parseInt(valueStr);
      } catch (NumberFormatException ne) {
        LOG.error("Invalid constraintValue string:" + valueStr + ". Use default value:"
            + Integer.MAX_VALUE);
      }
    }
    return value;
  }

  /**
   * constraints are selected in the order of the following rules: 1) don't select
   * constraints with CONSTRAINT_VALUE=ANY; 2) if one constraint is more specific than the
   * other, select the most specific one 3) if a message matches multiple constraints of
   * incomparable specificity, select the one with the minimum value 4) if a message
   * matches multiple constraints of incomparable specificity, and they all have the same
   * value, select the first in alphabetic order
   */
  Set<ConstraintItem> selectConstraints(Set<ConstraintItem> items,
      Map<ConstraintAttribute, String> attributes) {
    Map<String, ConstraintItem> selectedItems = new HashMap<String, ConstraintItem>();
    for (ConstraintItem item : items) {
      // don't select constraints with CONSTRAINT_VALUE=ANY
      if (item.getConstraintValue().equals(ConstraintValue.ANY.toString())) {
        continue;
      }

      String key = item.filter(attributes).toString();
      if (!selectedItems.containsKey(key)) {
        selectedItems.put(key, item);
      } else {
        ConstraintItem existingItem = selectedItems.get(key);
        if (existingItem.match(item.getAttributes())) {
          // item is more specific than existingItem
          selectedItems.put(key, item);
        } else if (!item.match(existingItem.getAttributes())) {
          // existingItem and item are of incomparable specificity
          int value = valueOf(item.getConstraintValue());
          int existingValue = valueOf(existingItem.getConstraintValue());
          if (value < existingValue) {
            // item's constraint value is less than that of existingItem
            selectedItems.put(key, item);
          } else if (value == existingValue) {
            if (item.toString().compareTo(existingItem.toString()) < 0) {
              // item is ahead of existingItem in alphabetic order
              selectedItems.put(key, item);
            }
          }
        }
      }
    }
    return new HashSet<ConstraintItem>(selectedItems.values());
  }

  @Override
  public void process(ClusterEvent event) throws Exception {
    Cluster cluster = event.getAttribute("ClusterDataCache");
    NewMessageOutput msgSelectionOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    Map<ResourceId, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());

    if (cluster == null || resourceMap == null || msgSelectionOutput == null) {
      throw new StageException("Missing attributes in event: " + event
          + ". Requires ClusterDataCache|RESOURCES|MESSAGES_SELECTED");
    }

    NewMessageOutput output = new NewMessageOutput();

    // TODO fix it
    ClusterConstraints constraint = cluster.getConstraint(ConstraintType.MESSAGE_CONSTRAINT);
    Map<String, Integer> throttleCounterMap = new HashMap<String, Integer>();

    if (constraint != null) {
      // go through all pending messages, they should be counted but not throttled
      for (ParticipantId participantId : cluster.getLiveParticipantMap().keySet()) {
        Participant liveParticipant = cluster.getLiveParticipantMap().get(participantId);
        throttle(throttleCounterMap, constraint, new ArrayList<Message>(liveParticipant
            .getMessageMap().values()), false);
      }
    }

    // go through all new messages, throttle if necessary
    // assume messages should be sorted by state transition priority in messageSelection stage
    for (ResourceId resourceId : resourceMap.keySet()) {
      Resource resource = resourceMap.get(resourceId);
      // TODO fix it
      for (PartitionId partitionId : resource.getPartitionMap().keySet()) {
        List<Message> messages = msgSelectionOutput.getMessages(resourceId, partitionId);
        if (constraint != null && messages != null && messages.size() > 0) {
          messages = throttle(throttleCounterMap, constraint, messages, true);
        }
        output.setMessages(resourceId, partitionId, messages);
      }
    }

    event.addAttribute(AttributeName.MESSAGES_THROTTLE.toString(), output);
  }

  private List<Message> throttle(Map<String, Integer> throttleMap, ClusterConstraints constraint,
      List<Message> messages, final boolean needThrottle) {

    List<Message> throttleOutputMsgs = new ArrayList<Message>();
    for (Message message : messages) {
      Map<ConstraintAttribute, String> msgAttr = ClusterConstraints.toConstraintAttributes(message);

      Set<ConstraintItem> matches = constraint.match(msgAttr);
      matches = selectConstraints(matches, msgAttr);

      boolean msgThrottled = false;
      for (ConstraintItem item : matches) {
        String key = item.filter(msgAttr).toString();
        if (!throttleMap.containsKey(key)) {
          throttleMap.put(key, valueOf(item.getConstraintValue()));
        }
        int value = throttleMap.get(key);
        throttleMap.put(key, --value);

        if (needThrottle && value < 0) {
          msgThrottled = true;

          if (LOG.isDebugEnabled()) {
            // TODO: printout constraint item that throttles the message
            LOG.debug("message: " + message + " is throttled by constraint: " + item);
          }
        }
      }

      if (!msgThrottled) {
        throttleOutputMsgs.add(message);
      }
    }

    return throttleOutputMsgs;
  }
}
