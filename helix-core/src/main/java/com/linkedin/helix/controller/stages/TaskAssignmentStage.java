/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.controller.stages;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.model.Resource;

public class TaskAssignmentStage extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(TaskAssignmentStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    long startTime = System.currentTimeMillis();
    logger.info("START TaskAssignmentStage.process()");

    HelixManager manager = event.getAttribute("helixmanager");
    Map<String, Resource> resourceMap = event
        .getAttribute(AttributeName.RESOURCES.toString());
    // MessageSelectionStageOutput messageOutput = event
    // .getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    MessageThrottleStageOutput messageOutput = event
        .getAttribute(AttributeName.MESSAGES_THROTTLE.toString());

    if (manager == null || resourceMap == null || messageOutput == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|RESOURCES|MESSAGES_THROTTLE");
    }

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    List<Message> messagesToSend = new ArrayList<Message>();
    for (String resourceName : resourceMap.keySet())
    {
      Resource resource = resourceMap.get(resourceName);
      for (Partition partition : resource.getPartitions())
      {
        List<Message> messages = messageOutput.getMessages(resourceName,
            partition);
        messagesToSend.addAll(messages);
      }
    }
    sendMessages(dataAccessor, messagesToSend);
    
    long endTime = System.currentTimeMillis();
    logger.info("END TaskAssignmentStage.process(). took: " + (endTime - startTime) + " ms");

  }

  protected void sendMessages(HelixDataAccessor dataAccessor,
      List<Message> messages)
  {
    if (messages == null || messages.size() == 0)
    {
      return;
    }
    Builder keyBuilder = dataAccessor.keyBuilder();
    List<PropertyKey> keys = new ArrayList<PropertyKey>();
    for (Message message : messages)
    {
      logger.info("Sending message " + message.getMsgId() + " to " + message.getTgtName() + " transition "
          + message.getPartitionName() + " from:" + message.getFromState()
          + " to:" + message.getToState());
      keys.add(keyBuilder.message(message.getTgtName(),message.getId()));
    }
    dataAccessor.createChildren(keys, messages);
    
  }
}
