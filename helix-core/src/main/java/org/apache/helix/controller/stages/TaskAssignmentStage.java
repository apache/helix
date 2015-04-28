package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

public class TaskAssignmentStage extends AbstractBaseStage {
  private static Logger logger = Logger.getLogger(TaskAssignmentStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    logger.info("START TaskAssignmentStage.process()");

    HelixManager manager = event.getAttribute("helixmanager");
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    MessageOutput messageOutput = event.getAttribute(AttributeName.MESSAGES_THROTTLE.toString());
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    Cluster cluster = event.getAttribute("Cluster");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    Map<ParticipantId, Participant> liveParticipantMap = cluster.getLiveParticipantMap();

    if (manager == null || resourceMap == null || messageOutput == null || cluster == null
        || cache == null || liveParticipantMap == null) {
      throw new StageException(
          "Missing attributes in event:"
              + event
              + ". Requires HelixManager|RESOURCES|MESSAGES_THROTTLE|BEST_POSSIBLE_STATE|Cluster|DataCache|liveInstanceMap");
    }

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    List<Message> messagesToSend = new ArrayList<Message>();
    for (ResourceId resourceId : resourceMap.keySet()) {
      for (PartitionId partitionId : bestPossibleStateOutput.getResourceAssignment(resourceId)
          .getMappedPartitionIds()) {
        List<Message> messages = messageOutput.getMessages(resourceId, partitionId);
        messagesToSend.addAll(messages);
      }
    }

    List<Message> outputMessages =
        batchMessage(dataAccessor.keyBuilder(), messagesToSend, resourceMap, liveParticipantMap,
            manager.getProperties());
    sendMessages(dataAccessor, outputMessages);

    long cacheStart = System.currentTimeMillis();
    cache.cacheMessages(outputMessages);
    long cacheEnd = System.currentTimeMillis();
    logger.debug("Caching messages took " + (cacheEnd - cacheStart) + " ms");

    long endTime = System.currentTimeMillis();
    logger.info("END TaskAssignmentStage.process(). took: " + (endTime - startTime) + " ms");

  }

  List<Message> batchMessage(Builder keyBuilder, List<Message> messages,
      Map<ResourceId, ResourceConfig> resourceMap,
      Map<ParticipantId, Participant> liveParticipantMap, HelixManagerProperties properties) {
    // group messages by its CurrentState path + "/" + fromState + "/" + toState
    Map<String, Message> batchMessages = new HashMap<String, Message>();
    List<Message> outputMessages = new ArrayList<Message>();

    Iterator<Message> iter = messages.iterator();
    while (iter.hasNext()) {
      Message message = iter.next();
      ResourceId resourceId = message.getResourceId();
      ResourceConfig resource = resourceMap.get(resourceId);

      ParticipantId participantId = ParticipantId.from(message.getTgtName());
      Participant liveParticipant = liveParticipantMap.get(participantId);
      String participantVersion = null;
      if (liveParticipant != null) {
        participantVersion = liveParticipant.getLiveInstance().getHelixVersion();
      }

      if (resource == null || resource.getIdealState() == null
          || !resource.getIdealState().getBatchMessageMode() || participantVersion == null
          || !properties.isFeatureSupported("batch_message", participantVersion)) {
        outputMessages.add(message);
        continue;
      }

      String key =
          keyBuilder.currentState(message.getTgtName(), message.getTypedTgtSessionId().stringify(),
              message.getResourceId().stringify()).getPath()
              + "/" + message.getTypedFromState() + "/" + message.getTypedToState();

      if (!batchMessages.containsKey(key)) {
        Message batchMessage = new Message(message.getRecord());
        batchMessage.setBatchMessageMode(true);
        outputMessages.add(batchMessage);
        batchMessages.put(key, batchMessage);
      }
      batchMessages.get(key).addPartitionName(message.getPartitionId().stringify());
    }

    return outputMessages;
  }

  protected void sendMessages(HelixDataAccessor dataAccessor, List<Message> messages) {
    if (messages == null || messages.isEmpty()) {
      return;
    }

    Builder keyBuilder = dataAccessor.keyBuilder();

    List<PropertyKey> keys = new ArrayList<PropertyKey>();
    for (Message message : messages) {
      logger.info("Sending Message " + message.getMessageId() + " to " + message.getTgtName()
          + " transit " + message.getPartitionId() + "|" + message.getPartitionIds() + " from:"
          + message.getTypedFromState() + " to:" + message.getTypedToState());

//       System.out.println("[dbg] Sending Message " + message.getMsgId() + " to "
//       + message.getTgtName() + " transit " + message.getPartitionId() + "|"
//       + message.getPartitionIds() + " from: " + message.getFromState() + " to: "
//       + message.getToState());

      keys.add(keyBuilder.message(message.getTgtName(), message.getId()));
    }

    dataAccessor.createChildren(keys, new ArrayList<Message>(messages));
  }
}
