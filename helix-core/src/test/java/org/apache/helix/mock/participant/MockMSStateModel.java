package org.apache.helix.mock.participant;

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

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// mock master-slave state model
@StateModelInfo(initialState = "OFFLINE", states = {
    "MASTER", "SLAVE", "ERROR"
})
public class MockMSStateModel extends StateModel {
  private static Logger LOG = LoggerFactory.getLogger(MockMSStateModel.class);

  protected MockTransition _transition;

  public MockMSStateModel(MockTransition transition) {
    _transition = transition;
  }

  public void setTransition(MockTransition transition) {
    _transition = transition;
  }

  @Transition(to = "*", from = "*")
  public void generalTransitionHandle(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info(String
        .format("Resource %s partition %s becomes %s from %s", message.getResourceName(),
            message.getPartitionName(), message.getToState(), message.getFromState()));
    if (_transition != null) {
      _transition.doTransition(message, context);
    }
  }

  @Override
  public void reset() {
    LOG.info("Default MockMSStateModel.reset() invoked");
    if (_transition != null) {
      _transition.doReset();
    }
  }
}
