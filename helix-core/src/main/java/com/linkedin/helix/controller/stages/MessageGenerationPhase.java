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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.StateModelDefinition;

/**
 * Compares the currentState,pendingState with IdealState and generate messages
 * 
 * @author kgopalak
 * 
 */
public class MessageGenerationPhase extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(MessageGenerationPhase.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    HelixManager manager = event.getAttribute("helixmanager");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());
    CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE
        .toString());
    BestPossibleStateOutput bestPossibleStateOutput = event
        .getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    if (manager == null || cache == null || resourceMap == null || currentStateOutput == null
        || bestPossibleStateOutput == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|DataCache|RESOURCES|CURRENT_STATE|BEST_POSSIBLE_STATE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    Map<String, String> sessionIdMap = new HashMap<String, String>();

    for (LiveInstance liveInstance : liveInstances.values())
    {
      sessionIdMap.put(liveInstance.getInstanceName(), liveInstance.getSessionId());
    }
    MessageGenerationOutput output = new MessageGenerationOutput();

    for (String resourceName : resourceMap.keySet())
    {
      Resource resource = resourceMap.get(resourceName);
      int bucketSize = resource.getBucketSize();

      StateModelDefinition stateModelDef = cache.getStateModelDef(resource.getStateModelDefRef());

      for (Partition partition : resource.getPartitions())
      {
        Map<String, String> instanceStateMap = bestPossibleStateOutput.getInstanceStateMap(
            resourceName, partition);

        for (String instanceName : instanceStateMap.keySet())
        {
          String desiredState = instanceStateMap.get(instanceName);

          String currentState = currentStateOutput.getCurrentState(resourceName, partition,
              instanceName);
          if (currentState == null)
          {
            currentState = stateModelDef.getInitialState();
          }

          if (desiredState.equalsIgnoreCase(currentState))
          {
            continue;
          }

          String pendingState = currentStateOutput.getPendingState(resourceName, partition,
              instanceName);

          String nextState = stateModelDef.getNextStateForTransition(currentState, desiredState);
          if (nextState == null)
          {
            logger.error("Unable to find a next state for partition: "
                + partition.getPartitionName() + " from stateModelDefinition"
                + stateModelDef.getClass() + " from:" + currentState + " to:" + desiredState);
            continue;
          }

          if (pendingState != null)
          {
            if (nextState.equalsIgnoreCase(pendingState))
            {
              logger.info("Message already exists for " + instanceName + " to transit "
                  + partition.getPartitionName() + " from " + currentState + " to " + nextState);
            } else if (currentState.equalsIgnoreCase(pendingState))
            {
              logger.info("Message hasn't been removed for " + instanceName + " to transit"
                  + partition.getPartitionName() + " to " + pendingState + ", desiredState: "
                  + desiredState);
            } else
            {
              logger.info("IdealState changed before state transition completes for "
                  + partition.getPartitionName() + " on " + instanceName + ", pendingState: "
                  + pendingState + ", currentState: " + currentState + ", nextState: " + nextState);
            }
          } else
          {
            Message message = createMessage(manager, resourceName, partition.getPartitionName(),
                instanceName, currentState, nextState, sessionIdMap.get(instanceName),
                stateModelDef.getId(), resource.getStateModelFactoryname(), bucketSize);
            IdealState idealState = cache.getIdealState(resourceName);
            // Set timeout of needed
            String stateTransition = currentState + "-" + nextState + "_"
                + Message.Attributes.TIMEOUT;
            if (idealState != null
                && idealState.getRecord().getSimpleField(stateTransition) != null)
            {
              try
              {
                int timeout = Integer.parseInt(idealState.getRecord().getSimpleField(
                    stateTransition));
                if (timeout > 0)
                {
                  message.setExecutionTimeout(timeout);
                }
              } catch (Exception e)
              {
                logger.error("", e);
              }
            }
            message.getRecord().setSimpleField("ClusterEventName", event.getName());
            output.addMessage(resourceName, partition, message);
          }
        }
      }
    }
    event.addAttribute(AttributeName.MESSAGES_ALL.toString(), output);
  }

  private Message createMessage(HelixManager manager, String resourceName, String partitionName,
      String instanceName, String currentState, String nextState, String sessionId,
      String stateModelDefName, String stateModelFactoryName, int bucketSize)
  {
    String uuid = UUID.randomUUID().toString();
    Message message = new Message(MessageType.STATE_TRANSITION, uuid);
    message.setSrcName(manager.getInstanceName());
    message.setTgtName(instanceName);
    message.setMsgState(MessageState.NEW);
    message.setPartitionName(partitionName);
    message.setResourceName(resourceName);
    message.setFromState(currentState);
    message.setToState(nextState);
    message.setTgtSessionId(sessionId);
    message.setSrcSessionId(manager.getSessionId());
    message.setStateModelDef(stateModelDefName);
    message.setStateModelFactoryName(stateModelFactoryName);
    message.setBucketSize(bucketSize);

    return message;
  }
}
