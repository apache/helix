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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.Message;

public class MessageOutput {

  private final Map<ResourceId, Map<PartitionId, List<Message>>> _messagesMap;

  public MessageOutput() {
    _messagesMap = new HashMap<ResourceId, Map<PartitionId, List<Message>>>();

  }

  public void addMessage(ResourceId resourceId, PartitionId partitionId, Message message) {
    if (!_messagesMap.containsKey(resourceId)) {
      _messagesMap.put(resourceId, new HashMap<PartitionId, List<Message>>());
    }
    if (!_messagesMap.get(resourceId).containsKey(partitionId)) {
      _messagesMap.get(resourceId).put(partitionId, new ArrayList<Message>());

    }
    _messagesMap.get(resourceId).get(partitionId).add(message);

  }

  public void setMessages(ResourceId resourceId, PartitionId partitionId,
      List<Message> selectedMessages) {
    if (!_messagesMap.containsKey(resourceId)) {
      _messagesMap.put(resourceId, new HashMap<PartitionId, List<Message>>());
    }
    _messagesMap.get(resourceId).put(partitionId, selectedMessages);

  }

  public List<Message> getMessages(ResourceId resourceId, PartitionId partitionId) {
    Map<PartitionId, List<Message>> map = _messagesMap.get(resourceId);
    if (map != null) {
      return map.get(partitionId);
    }
    return Collections.emptyList();

  }

  public Map<PartitionId, List<Message>> getMessages(ResourceId resourceId) {
    return _messagesMap.get(resourceId);
  }

  @Override
  public String toString() {
    return _messagesMap.toString();
  }
}
