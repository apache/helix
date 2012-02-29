package com.linkedin.helix.model;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.model.Constraint.ConstraintAttribute;
import com.linkedin.helix.model.Constraint.ConstraintItem;
import com.linkedin.helix.model.Constraint.ConstraintValue;
import com.linkedin.helix.model.Message.Attributes;
import com.linkedin.helix.model.Message.MessageType;

public class TestConstraint extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestConstraint.class);

  // TODO move to ClusterDataCache
  private int valueOf(String valueStr)
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
        LOG.error("Invalid constraint value token:" + valueStr
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
        LOG.error("Invalid constraint value string:" + valueStr
                  + ". Use default value:" + Integer.MAX_VALUE);
      }
    }
    return value;
  }

  // TODO move to MessageThrottleStage
  private Map<ConstraintAttribute, String> replace(ConstraintItem item,
                                                   Map<ConstraintAttribute, String> attributes)
  {
    Map<ConstraintAttribute, String> ret = new HashMap<ConstraintAttribute, String>();
    for (ConstraintAttribute key : item.getAttributes().keySet())
    {
      ret.put(key, attributes.get(key));
    }

    return ret;
  }

  /**
   * constraints are selected in the order of the following rules:
   *   1) don't select constraints with CONSTRAINT_VALUE=ANY;
   *   2) if one constraint is more specific than the other, select the most specific one
   *   3) if a message matches multiple constraints of the incomparable specificity,
   *   select the one with the minimum value
   *   4) if a message matches multiple constraints of the incomparable specificity,
   *   and they all have the same value, select the first in alphabetic order
   */
  private Set<ConstraintItem> selectConstraints(Set<ConstraintItem> items,
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

      String key = replace(item, attributes).toString();
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

  @Test
  public void testMsgConstraint()
  {
    String className = getShortClassName();
    System.out.println("START testMsgConstraint() at " + new Date(System.currentTimeMillis()));

    String clusterName = "CLUSTER_" + className + "_msg";
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    ZNRecord record = new ZNRecord("testMsgConstraint");

    // constraint0:
    // "MESSAGE_TYPE=STATE_TRANSITION,CONSTRAINT_VALUE=ANY"
    record.setMapField("constraint0", new TreeMap<String, String>());
    record.getMapField("constraint0").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint0").put("CONSTRAINT_VALUE", "ANY");
    ConstraintItem constraint0 = new ConstraintItem(record.getMapField("constraint0"));

    // constraint1:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,CONSTRAINT_VALUE=ANY"
    record.setMapField("constraint1", new TreeMap<String, String>());
    record.getMapField("constraint1").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint1").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint1").put("CONSTRAINT_VALUE", "50");
    ConstraintItem constraint1 = new ConstraintItem(record.getMapField("constraint1"));

    // constraint2:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=.*,RESOURCE=TestDB,CONSTRAINT_VALUE=2";
    record.setMapField("constraint2", new TreeMap<String, String>());
    record.getMapField("constraint2").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint2").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint2").put("INSTANCE", ".*");
    record.getMapField("constraint2").put("RESOURCE", "TestDB");
    record.getMapField("constraint2").put("CONSTRAINT_VALUE", "2");
    ConstraintItem constraint2 = new ConstraintItem(record.getMapField("constraint2"));

    // constraint3:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=localhost_12918,RESOURCE=.*,CONSTRAINT_VALUE=1";
    record.setMapField("constraint3", new TreeMap<String, String>());
    record.getMapField("constraint3").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint3").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint3").put("INSTANCE", "localhost_12919");
    record.getMapField("constraint3").put("RESOURCE", ".*");
    record.getMapField("constraint3").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint3 = new ConstraintItem(record.getMapField("constraint3"));

    // constraint4:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=.*,RESOURCE=.*,CONSTRAINT_VALUE=10"
    record.setMapField("constraint4", new TreeMap<String, String>());
    record.getMapField("constraint4").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint4").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint4").put("INSTANCE", ".*");
    record.getMapField("constraint4").put("RESOURCE", ".*");
    record.getMapField("constraint4").put("CONSTRAINT_VALUE", "10");
    ConstraintItem constraint4 = new ConstraintItem(record.getMapField("constraint4"));

    // constraint5:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=localhost_12918,RESOURCE=TestDB,CONSTRAINT_VALUE=5"
    record.setMapField("constraint5", new TreeMap<String, String>());
    record.getMapField("constraint5").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint5").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint5").put("INSTANCE", "localhost_12918");
    record.getMapField("constraint5").put("RESOURCE", "TestDB");
    record.getMapField("constraint5").put("CONSTRAINT_VALUE", "5");
    ConstraintItem constraint5 = new ConstraintItem(record.getMapField("constraint5"));

    DataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    accessor.setProperty(PropertyType.CONFIGS, record, ConfigScopeProperty.CLUSTER.toString(), "MESSAGE_CONSTRAINT");

    record = accessor.getProperty(PropertyType.CONFIGS, ConfigScopeProperty.CLUSTER.toString(), "MESSAGE_CONSTRAINT");
    Constraint constraint = new Constraint(record);
    // System.out.println("constraint: " + constraint);

    // message1: hit rule1 and rule2
    Message msg1 = createMessage(MessageType.STATE_TRANSITION,
                                "msgId-001",
                                "OFFLINE",
                                "SLAVE",
                                "TestDB",
                                "localhost_12918");

    Map<ConstraintAttribute, String> msgAttr = attributeOf(msg1);
    Set<ConstraintItem> matches = constraint.match(msgAttr);
    System.out.println(msg1 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 5);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint1));
    Assert.assertTrue(contains(matches, constraint2));
    Assert.assertTrue(contains(matches, constraint4));
    Assert.assertTrue(contains(matches, constraint5));

    matches = selectConstraints(matches, msgAttr);
    System.out.println(msg1 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 2);
    Assert.assertTrue(contains(matches, constraint1));
    Assert.assertTrue(contains(matches, constraint5));

    // message2: hit rule1, rule2, and rule3
    Message msg2 = createMessage(MessageType.STATE_TRANSITION,
                                 "msgId-002",
                                 "OFFLINE",
                                 "SLAVE",
                                 "TestDB",
                                 "localhost_12919");

    msgAttr = attributeOf(msg2);
    matches = constraint.match(msgAttr);
    System.out.println(msg2 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 5);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint1));
    Assert.assertTrue(contains(matches, constraint2));
    Assert.assertTrue(contains(matches, constraint3));
    Assert.assertTrue(contains(matches, constraint4));

    matches = selectConstraints(matches, msgAttr);
    System.out.println(msg2 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 2);
    Assert.assertTrue(contains(matches, constraint1));
    Assert.assertTrue(contains(matches, constraint3));

    System.out.println("END testMsgConstraint() at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testStateConstraint()
  {
    String className = getShortClassName();
    System.out.println("START testStateConstraint() at " + new Date(System.currentTimeMillis()));

    String clusterName = "CLUSTER_" + className + "_state";
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    ZNRecord record = new ZNRecord("testStateConstraint");

    // constraint0:
    // "STATE=MASTER,CONSTRAINT_VALUE=1"
    record.setMapField("constraint0", new TreeMap<String, String>());
    record.getMapField("constraint0").put("STATE", "MASTER");
    record.getMapField("constraint0").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint0 = new ConstraintItem(record.getMapField("constraint0"));

    // constraint1:
    // "STATE=MASTER,RESOURCE=TestDB,CONSTRAINT_VALUE=5"
    record.setMapField("constraint1", new TreeMap<String, String>());
    record.getMapField("constraint1").put("STATE", "MASTER");
    record.getMapField("constraint1").put("RESOURCE", "TestDB");
    record.getMapField("constraint1").put("CONSTRAINT_VALUE", "1");
    ConstraintItem constraint1 = new ConstraintItem(record.getMapField("constraint1"));

    // constraint2:
    // "STATE=MASTER,RESOURCE=.*,CONSTRAINT_VALUE=2"
    record.setMapField("constraint2", new TreeMap<String, String>());
    record.getMapField("constraint2").put("STATE", "MASTER");
    record.getMapField("constraint2").put("RESOURCE", ".*");
    record.getMapField("constraint2").put("CONSTRAINT_VALUE", "2");
    ConstraintItem constraint2 = new ConstraintItem(record.getMapField("constraint2"));

    DataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    accessor.setProperty(PropertyType.CONFIGS, record, ConfigScopeProperty.CLUSTER.toString(), "STATE_CONSTRAINT");

    record = accessor.getProperty(PropertyType.CONFIGS, ConfigScopeProperty.CLUSTER.toString(), "STATE_CONSTRAINT");
    Constraint constraint = new Constraint(record);
    // System.out.println("constraint: " + constraint);

    // state1: hit rule2
    Map<ConstraintAttribute, String> stateAttr1 = new HashMap<ConstraintAttribute, String>();
    stateAttr1.put(ConstraintAttribute.STATE, "MASTER");
    stateAttr1.put(ConstraintAttribute.RESOURCE, "TestDB");

    Set<ConstraintItem> matches = constraint.match(stateAttr1);
    System.out.println(stateAttr1 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 3);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint1));
    Assert.assertTrue(contains(matches, constraint2));

    matches = selectConstraints(matches, stateAttr1);
    System.out.println(stateAttr1 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 2);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint1));

    // state2: not hit any rules
    Map<ConstraintAttribute, String> stateAttr2 = new HashMap<ConstraintAttribute, String>();
    stateAttr2.put(ConstraintAttribute.STATE, "MASTER");
    stateAttr2.put(ConstraintAttribute.RESOURCE, "MyDB");

    matches = constraint.match(stateAttr2);
    System.out.println(stateAttr2 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 2);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint2));

    matches = selectConstraints(matches, stateAttr2);
    System.out.println(stateAttr2 + " matches(" + matches.size() + "): " + matches);
    Assert.assertEquals(matches.size(), 2);
    Assert.assertTrue(contains(matches, constraint0));
    Assert.assertTrue(contains(matches, constraint2));

    System.out.println("END testStateConstraint() at " + new Date(System.currentTimeMillis()));
  }

  private boolean contains(Set<ConstraintItem> constraints, ConstraintItem constraint)
  {
    for (ConstraintItem item : constraints)
    {
      if (item.toString().equals(constraint.toString()))
      {
        return true;
      }
    }
    return false;
  }

  private Message createMessage(MessageType type,
                                String msgId,
                                String fromState,
                                String toState,
                                String resourceName,
                                String tgtName)
  {
    Message msg = new Message(type.toString(), msgId);
    msg.setFromState(fromState);
    msg.setToState(toState);
    msg.getRecord().setSimpleField(Attributes.RESOURCE_NAME.toString(), resourceName);
    msg.setTgtName(tgtName);
    return msg;
  }

  // TODO move to Message
  // convert a message to attribute pairs
  private Map<ConstraintAttribute, String> attributeOf(Message msg)
  {
    Map<ConstraintAttribute, String> attributes = new TreeMap<ConstraintAttribute, String>();
    String msgType = msg.getMsgType();
    attributes.put(ConstraintAttribute.MESSAGE_TYPE, msgType);
    if (MessageType.STATE_TRANSITION.toString().equals(msgType))
    {
      if (msg.getFromState() != null && msg.getToState() != null)
      {
        attributes.put(ConstraintAttribute.TRANSITION,
            msg.getFromState() + "-" + msg.getToState());
      }
      if (msg.getResourceName() != null)
      {
        attributes.put(ConstraintAttribute.RESOURCE,
                   msg.getResourceName());
      }
      if (msg.getTgtName() != null)
      {
        attributes.put(ConstraintAttribute.INSTANCE, msg.getTgtName());
      }
    }
    return attributes;
  }

}
