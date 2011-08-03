package com.linkedin.clustermanager.controller.stages;

import java.util.List;

import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceKey;

public class MessageSelectionStageOutput
{

  public void addMessages(String resourceGroupName, ResourceKey resource,
      List<Message> selectedMessages)
  {
    
  }

  public List<Message> getMessages(String resourceGroupName,
      ResourceKey resource)
  {
    return null;
  }

}
