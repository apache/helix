package com.linkedin.clustermanager.controller.stages;

import java.util.HashMap;
import java.util.Map;

public class ClusterEvent
{
  Map<String, Object> _eventAttributeMap;

  public ClusterEvent()
  {
    _eventAttributeMap = new HashMap<String, Object>();
  }

  public void addAttribute(String attrName, Object attrValue)
  {
    _eventAttributeMap.put(attrName, attrValue);
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
