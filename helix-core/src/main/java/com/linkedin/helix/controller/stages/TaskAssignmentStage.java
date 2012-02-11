package com.linkedin.helix.controller.stages;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixAgent;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.ResourceGroup;
import com.linkedin.helix.model.ResourceKey;

public class TaskAssignmentStage extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(TaskAssignmentStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    HelixAgent manager = event.getAttribute("clustermanager");
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

    DataAccessor dataAccessor = manager.getDataAccessor();
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

  protected void sendMessages(DataAccessor dataAccessor,
      List<Message> messages)
  {
    if (messages == null || messages.size() == 0)
    {
      return;
    }

    for (Message message : messages)
    {
      logger.info("Sending message to " + message.getTgtName()
          + " transition " + message.getStateUnitKey() + " from:"
          + message.getFromState() + " to:" + message.getToState());
      dataAccessor.setProperty(PropertyType.MESSAGES,
                               message,
                               message.getTgtName(),
                               message.getId());
    }
  }
}
