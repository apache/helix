package com.linkedin.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.ResourceKey;

public class MessageSelectionStageOutput
{

  public MessageSelectionStageOutput()
  {
    _messagesMap = new HashMap<String, Map<ResourceKey, List<Message>>>();
  }

  private final Map<String, Map<ResourceKey, List<Message>>> _messagesMap;

  public void addMessages(String resourceGroupName, ResourceKey resource,
      List<Message> selectedMessages)
  {
    if (!_messagesMap.containsKey(resourceGroupName))
    {
      _messagesMap.put(resourceGroupName,
          new HashMap<ResourceKey, List<Message>>());
    }
    _messagesMap.get(resourceGroupName).put(resource, selectedMessages);

  }

  public List<Message> getMessages(String resourceGroupName,
      ResourceKey resource)
  {
    Map<ResourceKey, List<Message>> map = _messagesMap.get(resourceGroupName);
    if (map != null)
    {
      return map.get(resource);
    }
    return Collections.emptyList();

  }

}
