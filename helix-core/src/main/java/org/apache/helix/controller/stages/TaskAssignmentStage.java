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
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.log4j.Logger;

public class TaskAssignmentStage extends AbstractBaseStage {
  private static Logger logger = Logger.getLogger(TaskAssignmentStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    logger.info("START TaskAssignmentStage.process()");

    HelixManager manager = event.getAttribute("helixmanager");
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());
    MessageThrottleStageOutput messageOutput =
        event.getAttribute(AttributeName.MESSAGES_THROTTLE.toString());
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    Map<String, LiveInstance> liveInstanceMap = cache.getLiveInstances();

    if (manager == null || resourceMap == null || messageOutput == null || cache == null
        || liveInstanceMap == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|RESOURCES|MESSAGES_THROTTLE|DataCache|liveInstanceMap");
    }

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    List<Message> messagesToSend = new ArrayList<Message>();
    for (String resourceName : resourceMap.keySet()) {
      Resource resource = resourceMap.get(resourceName);
      for (Partition partition : resource.getPartitions()) {
        List<Message> messages = messageOutput.getMessages(resourceName, partition);
        messagesToSend.addAll(messages);
      }
    }

    List<Message> outputMessages =
        batchMessage(dataAccessor.keyBuilder(), messagesToSend, resourceMap, liveInstanceMap,
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
      Map<String, Resource> resourceMap, Map<String, LiveInstance> liveInstanceMap,
      HelixManagerProperties properties) {
    // group messages by its CurrentState path + "/" + fromState + "/" + toState
    Map<String, Message> batchMessages = new HashMap<String, Message>();
    List<Message> outputMessages = new ArrayList<Message>();

    Iterator<Message> iter = messages.iterator();
    while (iter.hasNext()) {
      Message message = iter.next();
      String resourceName = message.getResourceName();
      Resource resource = resourceMap.get(resourceName);

      String instanceName = message.getTgtName();
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);
      String participantVersion = null;
      if (liveInstance != null) {
        participantVersion = liveInstance.getHelixVersion();
      }

      if (resource == null || !resource.getBatchMessageMode() || participantVersion == null
          || !properties.isFeatureSupported("batch_message", participantVersion)) {
        outputMessages.add(message);
        continue;
      }

      String key =
          keyBuilder.currentState(message.getTgtName(), message.getTgtSessionId(),
              message.getResourceName()).getPath()
              + "/" + message.getFromState() + "/" + message.getToState();

      if (!batchMessages.containsKey(key)) {
        Message batchMessage = new Message(message.getRecord());
        batchMessage.setBatchMessageMode(true);
        outputMessages.add(batchMessage);
        batchMessages.put(key, batchMessage);
      }
      batchMessages.get(key).addPartitionName(message.getPartitionName());
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
      logger.info("Sending Message " + message.getMsgId() + " to " + message.getTgtName()
          + " transit " + message.getPartitionName() + "|" + message.getPartitionNames() + " from:"
          + message.getFromState() + " to:" + message.getToState());

      keys.add(keyBuilder.message(message.getTgtName(), message.getId()));
    }

    dataAccessor.createChildren(keys, new ArrayList<Message>(messages));
  }
}
