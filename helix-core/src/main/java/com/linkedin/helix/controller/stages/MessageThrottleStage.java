package com.linkedin.helix.controller.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.Constraint;
import com.linkedin.helix.model.Constraint.ConstraintAttribute;
import com.linkedin.helix.model.Constraint.ConstraintItem;
import com.linkedin.helix.model.Constraint.ConstraintType;
import com.linkedin.helix.model.Constraint.ConstraintValue;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.model.Resource;

public class MessageThrottleStage extends AbstractBaseStage
{
  private static final Logger LOG =
      Logger.getLogger(MessageThrottleStage.class.getName());

  int valueOf(String valueStr)
  {
    int value = Integer.MAX_VALUE;

    try
    {
      ConstraintValue valueToken = ConstraintValue.valueOf(valueStr);
      switch (valueToken)
      {
      case ANY:
        value = Integer.MAX_VALUE;
        break;
      default:
        LOG.error("Invalid constraintValue token:" + valueStr
                  + ". Use default value:" + Integer.MAX_VALUE);
        break;
      }
    }
    catch (Exception e)
    {
      try
      {
        value = Integer.parseInt(valueStr);
      }
      catch (NumberFormatException ne)
      {
        LOG.error("Invalid constraintValue string:" + valueStr
                  + ". Use default value:" + Integer.MAX_VALUE);
      }
    }
    return value;
  }

  /**
   * constraints are selected in the order of the following rules:
   *   1) don't select constraints with CONSTRAINT_VALUE=ANY;
   *   2) if one constraint is more specific than the other, select the most specific one
   *   3) if a message matches multiple constraints of incomparable specificity,
   *   select the one with the minimum value
   *   4) if a message matches multiple constraints of incomparable specificity,
   *   and they all have the same value, select the first in alphabetic order
   */
  Set<ConstraintItem> selectConstraints(Set<ConstraintItem> items,
                                        Map<ConstraintAttribute, String> attributes)
  {
    Map<String, ConstraintItem> selectedItems = new HashMap<String, ConstraintItem>();
    for (ConstraintItem item : items)
    {
      // don't select constraints with CONSTRAINT_VALUE=ANY
      if (item.getConstraintValue().equals(ConstraintValue.ANY.toString()))
      {
        continue;
      }

      String key = item.filter(attributes).toString();
      if (!selectedItems.containsKey(key))
      {
        selectedItems.put(key, item);
      }
      else
      {
        ConstraintItem existingItem = selectedItems.get(key);
        if (existingItem.match(item.getAttributes()))
        {
          // item is more specific than existingItem
          selectedItems.put(key, item);
        } else if (!item.match(existingItem.getAttributes()))
        {
          // existingItem and item are of incomparable specificity
          int value = valueOf(item.getConstraintValue());
          int existingValue = valueOf(existingItem.getConstraintValue());
          if ( value < existingValue)
          {
            // item's constraint value is less than that of existingItem
            selectedItems.put(key, item);
          } else if (value == existingValue)
          {
            if (item.toString().compareTo(existingItem.toString()) < 0)
            {
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
  public void process(ClusterEvent event) throws Exception
  {
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    MessageSelectionStageOutput msgSelectionOutput = event
        .getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    Map<String, Resource> resourceMap = event
        .getAttribute(AttributeName.RESOURCES.toString());

    if (cache == null || resourceMap == null || msgSelectionOutput == null)
    {
      throw new StageException("Missing attributes in event: " + event
          + ". Requires ClusterDataCache|RESOURCES|MESSAGES_SELECTED");
    }

    MessageThrottleStageOutput output = new MessageThrottleStageOutput();

    // TODO add cache.getMsgConstraint()
    Constraint constraint = cache.getConstraint(ConstraintType.MESSAGE_CONSTRAINT);
    Map<String, Integer> throttleMap = new HashMap<String, Integer>();

    if (constraint != null)
    {
      // go through all pending messages, they should be counted but not throttled
      for (String instance : cache.getLiveInstances().keySet())
      {
        throttle(throttleMap, constraint, new ArrayList<Message>(cache.getMessages(instance).values()), false);
      }
    }

    // go through all new messages, throttle if necessary
    for (String resourceName : resourceMap.keySet())
    {
      Resource resource = resourceMap.get(resourceName);
      for (Partition partition : resource.getPartitions())
      {
        List<Message> messages = msgSelectionOutput.getMessages(resourceName, partition);
        if (constraint != null)
        {
         messages = throttle(throttleMap, constraint, messages, true);
        }
        output.addMessages(resourceName, partition, messages);
      }
    }

    event.addAttribute(AttributeName.MESSAGES_THROTTLE.toString(), output);
  }

  private List<Message> throttle(Map<String, Integer> throttleMap,
                           Constraint constraint,
                           List<Message> messages,
                           final boolean needThrottle)
  {
    List<Message> throttledMsg = new ArrayList<Message>();
    for (Message message : messages)
    {
      Map<ConstraintAttribute, String> msgAttr = message.toConstraintAttributes();

      Set<ConstraintItem> matches = constraint.match(msgAttr);
      matches = selectConstraints(matches, msgAttr);

      boolean msgThrottled = false;
      for (ConstraintItem item : matches)
      {
        String key = item.filter(msgAttr).toString();
        if (!throttleMap.containsKey(key))
        {
          throttleMap.put(key, valueOf(item.getConstraintValue()));
        }
        int value = throttleMap.get(key);
        throttleMap.put(key, --value);

        if (needThrottle && value < 0)
        {
          msgThrottled = true;
        }
      }

      if (msgThrottled)
      {
        LOG.debug("throttled message: " + message);
      } else
      {
        throttledMsg.add(message);
      }
    }

    return throttledMsg;
  }
}
