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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;


public class MessageSelectionStageOutput
{
  private final Map<String, Map<Partition, List<Message>>> _messagesMap;

  public MessageSelectionStageOutput()
  {
    _messagesMap = new HashMap<String, Map<Partition, List<Message>>>();
  }

  public void addMessages(String resourceName, Partition partition,
      List<Message> selectedMessages)
  {
    if (!_messagesMap.containsKey(resourceName))
    {
      _messagesMap.put(resourceName,
          new HashMap<Partition, List<Message>>());
    }
    _messagesMap.get(resourceName).put(partition, selectedMessages);

  }

  public List<Message> getMessages(String resourceName,
      Partition partition)
  {
    Map<Partition, List<Message>> map = _messagesMap.get(resourceName);
    if (map != null)
    {
      return map.get(partition);
    }
    return Collections.emptyList();

  }

  @Override
  public String toString()
  {
    return _messagesMap.toString();
  }

}
