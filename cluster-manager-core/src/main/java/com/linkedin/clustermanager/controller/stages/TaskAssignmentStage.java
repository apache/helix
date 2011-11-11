package com.linkedin.clustermanager.controller.stages;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

public class TaskAssignmentStage extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(TaskAssignmentStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    MessageSelectionStageOutput messageSelectionStageOutput = event
        .getAttribute(AttributeName.MESSAGES_SELECTED.toString());

    if (manager == null || resourceGroupMap == null
        || messageSelectionStageOutput == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires ClusterManager|RESOURCE_GROUPS|MESSAGES_SELECTED");
    }

    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    for (String resourceGroupName : resourceGroupMap.keySet())
    {
      ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
      for (ResourceKey resource : resourceGroup.getResourceKeys())
      {
        List<Message> messages = messageSelectionStageOutput.getMessages(
            resourceGroupName, resource);
        sendMessages(dataAccessor, messages);
      }
    }
  }

  protected void sendMessages(ClusterDataAccessor dataAccessor,
      List<Message> messages)
  {
    if (messages == null || messages.size() == 0)
    {
      return;
    }
    for (Message message : messages)
    {
      logger.info(": Sending message to " + message.getTgtName()
          + " transition " + message.getStateUnitKey() + " from:"
          + message.getFromState() + " to:" + message.getToState());
      dataAccessor.setProperty(PropertyType.MESSAGES, message.getRecord(),
          message.getTgtName(), message.getId());
    }
  }
}
