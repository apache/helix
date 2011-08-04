package com.linkedin.clustermanager.controller.stages;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
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
    if (manager == null)
    {
      throw new StageException("ClusterManager attribute value is null");
    }
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    MessageSelectionStageOutput messageSelectionStageOutput = event
        .getAttribute(AttributeName.MESSAGES_SELECTED.toString());
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
    for (Message message : messages)
    {
      logger.info(": Sending message to " + message.getTgtName()
          + " transition " + message.getStateUnitKey() + " from:"
          + message.getFromState() + " to:" + message.getToState());
      dataAccessor.setInstanceProperty(message.getTgtName(),
          InstancePropertyType.MESSAGES, message.getId(), message);
    }
  }
}
