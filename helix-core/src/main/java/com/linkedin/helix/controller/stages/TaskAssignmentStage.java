package com.linkedin.helix.controller.stages;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.Partition;

public class TaskAssignmentStage extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(TaskAssignmentStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    HelixManager manager = event.getAttribute("helixmanager");
    Map<String, Resource> resourceMap = event
        .getAttribute(AttributeName.RESOURCES.toString());
    MessageSelectionStageOutput messageSelectionStageOutput = event
        .getAttribute(AttributeName.MESSAGES_SELECTED.toString());

    if (manager == null || resourceMap == null
        || messageSelectionStageOutput == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|RESOURCES|MESSAGES_SELECTED");
    }

    DataAccessor dataAccessor = manager.getDataAccessor();
    for (String resourceName : resourceMap.keySet())
    {
      Resource resource = resourceMap.get(resourceName);
      for (Partition partition : resource.getPartitions())
      {
        List<Message> messages = messageSelectionStageOutput.getMessages(
            resourceName, partition);
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
          + " transition " + message.getPartitionName() + " from:"
          + message.getFromState() + " to:" + message.getToState());
      dataAccessor.setProperty(PropertyType.MESSAGES,
                               message,
                               message.getTgtName(),
                               message.getId());
    }
  }
}
