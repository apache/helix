package com.linkedin.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Partition;

public class MessageSelectionStageOutput
{

  public MessageSelectionStageOutput()
  {
    _messagesMap = new HashMap<String, Map<Partition, List<Message>>>();
  }

  private final Map<String, Map<Partition, List<Message>>> _messagesMap;

  public void addMessages(String resourceName, Partition resource,
      List<Message> selectedMessages)
  {
    if (!_messagesMap.containsKey(resourceName))
    {
      _messagesMap.put(resourceName,
          new HashMap<Partition, List<Message>>());
    }
    _messagesMap.get(resourceName).put(resource, selectedMessages);

  }

  public List<Message> getMessages(String resourceName,
      Partition resource)
  {
    Map<Partition, List<Message>> map = _messagesMap.get(resourceName);
    if (map != null)
    {
      return map.get(resource);
    }
    return Collections.emptyList();

  }

}
