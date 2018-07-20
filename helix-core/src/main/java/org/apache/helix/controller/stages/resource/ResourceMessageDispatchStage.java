package org.apache.helix.controller.stages.resource;

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

import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.MessageDispatchStage;
import org.apache.helix.controller.stages.MessageOutput;

public class ResourceMessageDispatchStage extends MessageDispatchStage {

  @Override
  public void process(ClusterEvent event) throws Exception {
    MessageOutput messageOutput =
        event.getAttribute(AttributeName.MESSAGES_THROTTLE.name());
    processEvent(event, messageOutput);
  }
}
