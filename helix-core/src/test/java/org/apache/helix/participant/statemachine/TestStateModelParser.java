package org.apache.helix.participant.statemachine;

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

import java.lang.reflect.Method;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestStateModelParser {

  private static Logger LOG = Logger.getLogger(TestStateModelParser.class);

  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "ERROR"
  })
  class StateModelUsingAnnotation extends TransitionHandler {
    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      LOG.info("Become SLAVE from OFFLINE");
    }

    @Override
    @Transition(to = "DROPPED", from = "ERROR")
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      LOG.info("Become DROPPED from ERROR");
    }

  }

  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "ERROR"
  })
  class DerivedStateModelUsingAnnotation extends StateModelUsingAnnotation {
    @Transition(to = "SLAVE", from = "OFFLINE")
    public void derivedOnBecomeSlaveFromOffline(Message message, NotificationContext context) {
      LOG.info("Derived Become SLAVE from OFFLINE");
    }
  }

  class StateModelUsingNameConvention extends TransitionHandler {
    // empty state model
  }

  @Test
  public void testUsingAnnotation() {
    StateModelParser parser = new StateModelParser();
    StateModelUsingAnnotation testModel = new StateModelUsingAnnotation();

    Method method =
        parser.getMethodForTransitionUsingAnnotation(testModel.getClass(), "offline", "slave",
            new Class[] {
                Message.class, NotificationContext.class
            });

    // System.out.println("method-name: " + method.getName());
    Assert.assertNotNull(method);
    Assert.assertEquals(method.getName(), "onBecomeSlaveFromOffline");
  }

  @Test
  public void testDerivedUsingAnnotation() {
    StateModelParser parser = new StateModelParser();
    DerivedStateModelUsingAnnotation testModel = new DerivedStateModelUsingAnnotation();

    Method method =
        parser.getMethodForTransitionUsingAnnotation(testModel.getClass(), "offline", "slave",
            new Class[] {
                Message.class, NotificationContext.class
            });

    // System.out.println("method-name: " + method.getName());
    Assert.assertNotNull(method);
    Assert.assertEquals(method.getName(), "derivedOnBecomeSlaveFromOffline");

    method =
        parser.getMethodForTransitionUsingAnnotation(testModel.getClass(), "error", "dropped",
            new Class[] {
                Message.class, NotificationContext.class
            });

    // System.out.println("method: " + method);
    Assert.assertNotNull(method);
    Assert.assertEquals(method.getName(), "onBecomeDroppedFromError");

  }

  @Test
  public void testUsingNameConvention() {
    StateModelParser parser = new StateModelParser();
    StateModelUsingNameConvention testModel = new StateModelUsingNameConvention();

    Method method =
        parser.getMethodForTransition(testModel.getClass(), "error", "dropped", new Class[] {
            Message.class, NotificationContext.class
        });
    Assert.assertNotNull(method);
    Assert.assertEquals(method.getName(), "onBecomeDroppedFromError");

  }
}
