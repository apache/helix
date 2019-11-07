package org.apache.helix.mock;

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

import java.util.List;
import java.util.Map;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.InstanceType;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;

public class MockClusterMessagingService implements ClusterMessagingService {
  @Override
  public int send(Criteria recipientCriteria, Message message) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int send(Criteria receipientCriteria, Message message, AsyncCallback callbackOnReply,
      int timeOut) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int sendAndWait(Criteria receipientCriteria, Message message,
      AsyncCallback callbackOnReply, int timeOut) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory) {
    // TODO Auto-generated method stub

  }

  @Override public void registerMessageHandlerFactory(List<String> types,
      MessageHandlerFactory factory) {
    // TODO Auto-generated method stub
  }

  @Override
  public int send(Criteria receipientCriteria, Message message, AsyncCallback callbackOnReply,
      int timeOut, int retryCount) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int sendAndWait(Criteria receipientCriteria, Message message,
      AsyncCallback callbackOnReply, int timeOut, int retryCount) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Map<InstanceType, List<Message>> generateMessage(Criteria recipientCriteria,
      Message messageTemplate) {
    // TODO Auto-generated method stub
    return null;
  }

}
