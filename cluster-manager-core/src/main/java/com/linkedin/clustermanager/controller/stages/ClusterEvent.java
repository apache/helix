package com.linkedin.clustermanager.controller.stages;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ClusterEvent
{
  private static final Logger logger = Logger.getLogger(ClusterEvent.class
      .getName());
  private final String _name;
  private Map<String, Object> _eventAttributeMap;

  public ClusterEvent(String name)
  {
    _name = name;
    _eventAttributeMap = new HashMap<String, Object>();
  }

  public void addAttribute(String attrName, Object attrValue)
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("Adding attribute:" + attrName);
      logger.debug(" attribute value:" + attrValue);
    }
    _eventAttributeMap.put(attrName, attrValue);
  }

  public String getName()
  {
    return _name;
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T getAttribute(String attrName)
  {
    Object ret = _eventAttributeMap.get(attrName);
    if (ret != null)
    {
      return (T) ret;
    }
    return null;
  }
}
