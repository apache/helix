package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceKey;

public class MessageGenerationOutput
{

  private final Map<String, Map<ResourceKey, List<Message>>> _messagesMap;

  public MessageGenerationOutput()
  {
    _messagesMap = new HashMap<String, Map<ResourceKey, List<Message>>>();

  }

  public void addMessage(String resourceGroupName, ResourceKey resource,
      Message message)
  {
    if (!_messagesMap.containsKey(resourceGroupName))
    {
      _messagesMap.put(resourceGroupName,
          new HashMap<ResourceKey, List<Message>>());
    }
    if (!_messagesMap.get(resourceGroupName).containsKey(resource))
    {
      _messagesMap.get(resourceGroupName).put(resource,
          new ArrayList<Message>());

    }
    _messagesMap.get(resourceGroupName).get(resource).add(message);

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
