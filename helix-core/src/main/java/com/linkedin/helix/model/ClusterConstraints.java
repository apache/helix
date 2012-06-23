/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.Message.MessageType;

public class ClusterConstraints extends HelixProperty
{
  private static Logger LOG = Logger.getLogger(ClusterConstraints.class);

  public enum ConstraintAttribute
  {
    STATE, MESSAGE_TYPE, TRANSITION, RESOURCE, INSTANCE, CONSTRAINT_VALUE
  }

  public enum ConstraintValue
  {
    ANY
  }

  public enum ConstraintType
  {
    STATE_CONSTRAINT, MESSAGE_CONSTRAINT
  }

  static public class ConstraintItem
  {
    // attributes e.g. {STATE:MASTER, RESOURCEG:TestDB, INSTANCE:localhost_12918}
    final Map<ConstraintAttribute, String> _attributes;
    String _constraintValue;

    public ConstraintItem(Map<String, String> attributes)
    {
      _attributes = new TreeMap<ConstraintAttribute, String>();
      _constraintValue = null;

      if (attributes != null)
      {
        for (String key : attributes.keySet())
        {
          try
          {
            ConstraintAttribute attr = ConstraintAttribute.valueOf(key);
            if (attr == ConstraintAttribute.CONSTRAINT_VALUE)
            {
              String value = attributes.get(key);
              try
              {
                ConstraintValue.valueOf(value);
              } catch (Exception e)
              {
                try
                {
                  Integer.parseInt(value);
                }
                catch (NumberFormatException ne)
                {
                  LOG.error("Invalid constraintValue " + key + ":" + value);
                  continue;
                }
              }
              _constraintValue = attributes.get(key);
            } else
            {
              _attributes.put(attr, attributes.get(key));
            }
          } catch (Exception e)
          {
            LOG.error("Invalid constraintAttribute " + key + ":" + attributes.get(key));
            continue;
          }
        }
      }
    }

    public boolean match(Map<ConstraintAttribute, String> attributes)
    {
      for (ConstraintAttribute key : _attributes.keySet())
      {
        if (!attributes.containsKey(key))
        {
          return false;
        }

        if (!attributes.get(key).matches(_attributes.get(key)))
        {
          return false;
        }
      }
      return true;
    }

    // filter out attributes that are not specified by this constraint
    public Map<ConstraintAttribute, String> filter(Map<ConstraintAttribute, String> attributes)
    {
      Map<ConstraintAttribute, String> ret = new HashMap<ConstraintAttribute, String>();
      for (ConstraintAttribute key : _attributes.keySet())
      {
        // TODO: what if attributes.get(key)==null? might need match function at constrait level  
        ret.put(key, attributes.get(key));
      }

      return ret;
    }

    public String getConstraintValue()
    {
      return _constraintValue;
    }

    public Map<ConstraintAttribute, String> getAttributes()
    {
      return _attributes;
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append(_attributes + ":" + _constraintValue);
      return sb.toString();
    }
  }

  private final List<ConstraintItem> _constraints = new ArrayList<ConstraintItem>();

  public ClusterConstraints(ZNRecord record)
  {
    super(record);

    for (String key : _record.getMapFields().keySet())
    {
      ConstraintItem item = new ConstraintItem(_record.getMapField(key));
      if (item.getAttributes().size() > 0 && item.getConstraintValue() != null)
      {
        _constraints.add(item);
      } else
      {
        LOG.error("Invalid constraint " + key + ":" + _record.getMapField(key));
      }
    }
  }

  /**
   * return a set of constraints that match the attribute pairs
   */
  public Set<ConstraintItem> match(Map<ConstraintAttribute, String> attributes)
  {
    Set<ConstraintItem> matches = new HashSet<ConstraintItem>();
    for (ConstraintItem item : _constraints)
    {
      if (item.match(attributes))
      {
        matches.add(item);
      }
    }
    return matches;
  }

  // convert a message to constraint attribute pairs
  public static Map<ConstraintAttribute, String> toConstraintAttributes(Message msg)
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
        attributes.put(ConstraintAttribute.RESOURCE, msg.getResourceName());
      }
      if (msg.getTgtName() != null)
      {
        attributes.put(ConstraintAttribute.INSTANCE, msg.getTgtName());
      }
    }
    return attributes;
  }

  @Override
  public boolean isValid()
  {
    // TODO Auto-generated method stub
    return true;
  }

}
