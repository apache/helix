package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

public class MessageSelectionStage extends AbstractBaseStage
{

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    if (manager == null)
    {
      throw new StageException("ClusterManager attribute value is null");
    }
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    List<ZNRecord> stateModelDefs = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.STATEMODELDEFS);
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    MessageGenerationOutput messageGenOutput = event
        .getAttribute(AttributeName.MESSAGES_ALL.toString());
    MessageSelectionStageOutput output = new MessageSelectionStageOutput();
    for (String resourceGroupName : resourceGroupMap.keySet())
    {
      ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
      StateModelDefinition stateModelDef = lookupStateModel(
          resourceGroup.getStateModelDefRef(), stateModelDefs);
      for (ResourceKey resource : resourceGroup.getResourceKeys())
      {
        List<Message> messages = messageGenOutput.getMessages(
            resourceGroupName, resource);
        List<Message> selectedMessages = selectMessages(messages, stateModelDef);
        output.addMessages(resourceGroupName, resource, selectedMessages);
      }
    }
    event.addAttribute(AttributeName.MESSAGES_SELECTED.toString(), output);
  }

  protected List<Message> selectMessages(List<Message> messages,
      StateModelDefinition stateModelDef)
  {
    if (messages == null || messages.size() == 0)
    {
      return Collections.emptyList();
    }
    
    Set<String> possibleTransitions = new HashSet<String>();
    for (Message message : messages)
    {
      String transition = message.getFromState() + "-" + message.getToState();
      possibleTransitions.add(transition.toUpperCase());
    }
    String preferredTransition = null;
    List<String> stateTransitionPriorityList = stateModelDef
        .getStateTransitionPriorityList();
    
    for (String transition : stateTransitionPriorityList)
    {
      if (possibleTransitions.contains(transition.toUpperCase()))
      {
        preferredTransition = transition;
        break;
      }
    }
    if (preferredTransition != null)
    {
      List<Message> messagesToSend = new ArrayList<Message>();
      for (Message message : messages)
      {
        String transition = message.getFromState() + "-" + message.getToState();
        if (transition.equalsIgnoreCase(preferredTransition))
        {
          messagesToSend.add(message);
        }
      }
      return messagesToSend;
    }
    return Collections.emptyList();
  }

  private StateModelDefinition lookupStateModel(String stateModelDefRef,
      List<ZNRecord> stateModelDefs)
  {
    for (ZNRecord record : stateModelDefs)
    {
      if (record.getId().equals(stateModelDefRef))
      {
        return new StateModelDefinition(record);
      }
    }
    return null;
  }
}
