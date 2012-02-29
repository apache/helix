package com.linkedin.helix.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;
import com.linkedin.helix.model.Message.MessageType;

public class MessageConstraint extends ZNRecordDecorator
{
  private static Logger LOG = Logger.getLogger(MessageConstraint.class);

  public enum MsgConstraintAttribute
  {
    MESSAGE_TYPE, TRANSITION, RESOURCE, INSTANCE, CONSTRAINT_VALUE
  }

  public enum MsgConstraintValue
  {
    ANY
  }

  public class MessageConstraintItem
  {
    // pairs: e.g. RESOURCEGROUP:TestDB, INSTANCE:localhost_12918, CONSTRAINT_VALUE:10
    final Map<MsgConstraintAttribute, String> _attrPairs;
    String _constraintValue;

    public MessageConstraintItem(Map<String, String> pairs)
    {
      _attrPairs = new TreeMap<MsgConstraintAttribute, String>();
      _constraintValue = null;

      if (pairs != null)
      {
        for (String key : pairs.keySet())
        {
          try
          {
            MsgConstraintAttribute attr = MsgConstraintAttribute.valueOf(key);
            if (attr == MsgConstraintAttribute.CONSTRAINT_VALUE)
            {
              _constraintValue = pairs.get(key);
            } else
            {
              _attrPairs.put(attr, pairs.get(key));
            }
          } catch (Exception e)
          {
            LOG.error("invalide msgConstraintAttribute " + key + ":" + pairs.get(key));
            continue;
          }
        }
      }
    }

    private MessageConstraintItem(Map<MsgConstraintAttribute, String> attrPairs, String constraintValue)
    {
      _attrPairs = attrPairs;
      _constraintValue = constraintValue;
    }

    // convert a message to <attribute:value> pairs
    private Map<MsgConstraintAttribute, String> convert(Message msg)
    {
      Map<MsgConstraintAttribute, String> msgPairs = new TreeMap<MsgConstraintAttribute, String>();
      String msgType = msg.getMsgType();
      msgPairs.put(MsgConstraintAttribute.MESSAGE_TYPE, msgType);
      if (MessageType.STATE_TRANSITION.toString().equals(msgType))
      {
        if (msg.getFromState() != null && msg.getToState() != null)
        {
          msgPairs.put(MsgConstraintAttribute.TRANSITION,
              msg.getFromState() + "-" + msg.getToState());
        }
        if (msg.getResourceName() != null)
        {
          msgPairs.put(MsgConstraintAttribute.RESOURCE,
                     msg.getResourceName());
        }
        if (msg.getTgtName() != null)
        {
          msgPairs.put(MsgConstraintAttribute.INSTANCE, msg.getTgtName());
        }
      }
      return msgPairs;
    }

    public MessageConstraintItem match(Message message)
    {
      Map<MsgConstraintAttribute, String> msgPairs = convert(message);
      Map<MsgConstraintAttribute, String> matchedPairs = new TreeMap<MsgConstraintAttribute, String>();
      for (MsgConstraintAttribute key : _attrPairs.keySet())
      {
        if (!msgPairs.containsKey(key))
        {
          return null;
        }

        if (!msgPairs.get(key).matches(_attrPairs.get(key)))
        {
          return null;
        }
        matchedPairs.put(key, msgPairs.get(key));
      }

      return new MessageConstraintItem(matchedPairs, _constraintValue);
    }

    public String getConstraintValue()
    {
      return _constraintValue;
    }

    public Map<MsgConstraintAttribute, String> getAttrPairs()
    {
      return _attrPairs;
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append(_attrPairs + ":" + _constraintValue);
      return sb.toString();
    }
  }

  private final List<MessageConstraintItem> _constraints = new ArrayList<MessageConstraintItem>();

  public MessageConstraint(ZNRecord record)
  {
    super(record);

    for (String key : _record.getMapFields().keySet())
    {
      MessageConstraintItem item = new MessageConstraintItem(_record.getMapField(key));
      if (item.getAttrPairs().size() > 0 && item.getConstraintValue() != null)
      {
        _constraints.add(item);
      } else
      {
        LOG.error("invalid constraint " + key + ":" + _record.getMapField(key));
      }
    }
  }

  /**
   * return a set of messageConstraints that match the message
   */
  public Set<MessageConstraintItem> match(Message message)
  {
    Set<MessageConstraintItem> matches = new HashSet<MessageConstraintItem>();
    for (MessageConstraintItem item : _constraints)
    {
      MessageConstraintItem match = item.match(message);
      if (match != null)
      {
        matches.add(match);
      }
    }
    return matches;
  }

  @Override
  public boolean isValid()
  {
    // TODO Auto-generated method stub
    return true;
  }

}
