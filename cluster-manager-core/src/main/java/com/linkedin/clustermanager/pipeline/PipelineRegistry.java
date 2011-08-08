package com.linkedin.clustermanager.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineRegistry
{
  Map<String, List<Pipeline>> _map;

  public PipelineRegistry()
  {
    _map = new HashMap<String, List<Pipeline>>();
  }

  public void register(String eventName, Pipeline... pipelines)
  {
    if (!_map.containsKey(eventName))
    {
      _map.put(eventName, new ArrayList<Pipeline>());
    }
    List<Pipeline> list = _map.get(eventName);
    for (Pipeline pipeline : pipelines)
    {
      list.add(pipeline);
    }
  }

  public List<Pipeline> getPipelinesForEvent(String eventName)
  {
    if (_map.containsKey(eventName))
    {
      return _map.get(eventName);
    }
    return Collections.emptyList();
  }
}
