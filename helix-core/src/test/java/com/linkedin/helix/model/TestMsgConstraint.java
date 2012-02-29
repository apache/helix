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
import com.linkedin.helix.model.Message.Attributes;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.model.MessageConstraint.MessageConstraintItem;
import com.linkedin.helix.model.MessageConstraint.MsgConstraintValue;

public class TestMsgConstraint extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestMsgConstraint.class);

  private int valueOf(String valueStr)
  {
    int value = Integer.MAX_VALUE;

    try
    {
      MsgConstraintValue valueToken = MsgConstraintValue.valueOf(valueStr);
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

  /**
   * constraint filter rules: 1) items with CONSTRAINT_VALUE=ANY; 2) if a message matches
   * multiple similar items, keep the one with the minimum value only. e.g. Message
   * (RESOURCE_NAME:TestDB) matches both constraint1 (RESOURCE:.*, CONSTRAINT_VALUE:10)
   * and constraint2 (RESOURCE:TestDB, CONSTRAINT_VALUE:1). They are same except the value
   * part after replacing *. Then filter constraint1.
   */
  private Set<MessageConstraintItem> filterMsgConstraints(Set<MessageConstraintItem> matches)
  {
    Map<String, MessageConstraintItem> filteredMatches =
        new HashMap<String, MessageConstraintItem>();
    for (MessageConstraintItem item : matches)
    {
      if (item.getConstraintValue().equals(MsgConstraintValue.ANY.toString()))
      {
        continue;
      }

      String key = item.getAttrPairs().toString();
      if (!filteredMatches.containsKey(key))
      {
        filteredMatches.put(key, item);
      }
      else
      {
        MessageConstraintItem existItem = filteredMatches.get(key);
        if (valueOf(existItem.getConstraintValue()) > valueOf(item.getConstraintValue()))
        {
          filteredMatches.put(key, item);
        }
      }
    }
    return new HashSet<MessageConstraintItem>(filteredMatches.values());
  }

  @Test
  public void testMsgConstraint()
  {
    String className = getShortClassName();
    System.out.println("START " + className + " at " + new Date(System.currentTimeMillis()));

    String clusterName = "CLUSTER_" + className;
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    ZNRecord record = new ZNRecord("testMsgConstraint");

    // constraint1:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,CONSTRAINT_VALUE=ANY"
    record.setMapField("constraint1", new TreeMap<String, String>());
    record.getMapField("constraint1").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint1").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint1").put("CONSTRAINT_VALUE", "ANY");

    // constraint2:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=.*,RESOURCE=TestDB,CONSTRAINT_VALUE=2";
    record.setMapField("constraint2", new TreeMap<String, String>());
    record.getMapField("constraint2").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint2").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint2").put("INSTANCE", ".*");
    record.getMapField("constraint2").put("RESOURCE", "TestDB");
    record.getMapField("constraint2").put("CONSTRAINT_VALUE", "2");

    // constraint3:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=localhost_12918,RESOURCE=.*";
    record.setMapField("constraint3", new TreeMap<String, String>());
    record.getMapField("constraint3").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint3").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint3").put("INSTANCE", "localhost_12918");
    record.getMapField("constraint3").put("RESOURCE", ".*");
    record.getMapField("constraint3").put("CONSTRAINT_VALUE", "1");

    // constraint4:
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=.*,RESOURCE_GROUP=.*"
    record.setMapField("constraint4", new TreeMap<String, String>());
    record.getMapField("constraint4").put("MESSAGE_TYPE", "STATE_TRANSITION");
    record.getMapField("constraint4").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint4").put("INSTANCE", ".*");
    record.getMapField("constraint4").put("RESOURCE", ".*");
    record.getMapField("constraint4").put("CONSTRAINT_VALUE", "10");

    DataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    accessor.setProperty(PropertyType.CONFIGS, record, ConfigScopeProperty.CLUSTER.toString(), "MESSAGE_CONSTRAINT");

    record = accessor.getProperty(PropertyType.CONFIGS, ConfigScopeProperty.CLUSTER.toString(), "MESSAGE_CONSTRAINT");
    MessageConstraint constraint = new MessageConstraint(record);
    // System.out.println("constraint: " + constraint);

    Map<Message, Integer> msgs = new HashMap<Message, Integer>();
    msgs.put(createMessage(MessageType.STATE_TRANSITION,
                           "msgId-001",
                           "OFFLINE",
                           "SLAVE",
                           "TestDB",
                           "localhost_12918"),
             4);
    msgs.put(createMessage(MessageType.STATE_TRANSITION,
                           "msgId-002",
                           "OFFLINE",
                           "SLAVE",
                           "TestDB",
                           "localhost_12919"),
             3);

    Map<String, Integer> throttleMap = new HashMap<String, Integer>();
    for (Message msg : msgs.keySet())
    {
      Set<MessageConstraintItem> matches = constraint.match(msg);
      System.out.println(msg + " matches(" + matches.size() + "): " + matches);
      Assert.assertEquals(matches.size(), msgs.get(msg).intValue());

      matches = filterMsgConstraints(matches);

      // simulate throttling stage
      for (MessageConstraintItem item : matches)
      {
        String key = item.toString();
        if (!throttleMap.containsKey(key))
        {
          throttleMap.put(key, valueOf(item.getConstraintValue()));
        }
        int value = throttleMap.get(key);
        throttleMap.put(key, --value);
      }
    }
    System.out.println("throttleMap: " + throttleMap);

    Assert.assertEquals(throttleMap.get("{MESSAGE_TYPE=STATE_TRANSITION, TRANSITION=OFFLINE-SLAVE, RESOURCE=TestDB, INSTANCE=localhost_12918}:1")
                                   .intValue(),
                        0);
    Assert.assertEquals(throttleMap.get("{MESSAGE_TYPE=STATE_TRANSITION, TRANSITION=OFFLINE-SLAVE, RESOURCE=TestDB, INSTANCE=localhost_12919}:2")
                                   .intValue(),
                        1);

    System.out.println("END " + className + " at " + new Date(System.currentTimeMillis()));
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
}
