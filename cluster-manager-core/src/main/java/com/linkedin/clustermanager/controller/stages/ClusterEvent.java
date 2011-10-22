package com.linkedin.clustermanager.controller.stages;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ClusterEvent
{
  private static final Logger logger = Logger.getLogger(ClusterEvent.class
      .getName());
  private final String _eventName;
  private final Map<String, Object> _eventAttributeMap;

  public ClusterEvent(String name)
  {
    _eventName = name;
    _eventAttributeMap = new HashMap<String, Object>();
  }

  public void addAttribute(String attrName, Object attrValue)
  {
    if (logger.isTraceEnabled())
    {
      logger.trace("Adding attribute:" + attrName);
      logger.trace(" attribute value:" + attrValue);
    }
    _eventAttributeMap.put(attrName, attrValue);
  }

  public String getName()
  {
    return _eventName;
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
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("name:"+ _eventName).append("\n");
    for(String key:_eventAttributeMap.keySet()){
      sb.append(key).append(":").append(_eventAttributeMap.get(key)).append("\n");
    }
    return sb.toString();
  }
}
