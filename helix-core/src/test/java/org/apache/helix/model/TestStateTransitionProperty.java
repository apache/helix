package org.apache.helix.model;

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

import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.StateTransitionTimeoutConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestStateTransitionProperty {

  @Test
  public void testTimeoutSetAndGet() {
    StateTransitionTimeoutConfig stateTransitionTimeoutConfig = new StateTransitionTimeoutConfig(new ZNRecord("TEST"));
    stateTransitionTimeoutConfig.setStateTransitionTimeout("MASTER", "SLAVE", 300);
    Assert.assertEquals(stateTransitionTimeoutConfig.getStateTransitionTimeout("MASTER", "SLAVE"), 300);

    stateTransitionTimeoutConfig.setStateTransitionTimeout("*", "MASTER", 500);
    Assert.assertEquals(stateTransitionTimeoutConfig.getStateTransitionTimeout("OFFLINE", "MASTER"), 500);

    Assert.assertEquals(stateTransitionTimeoutConfig.getStateTransitionTimeout("SLAVE", "OFFLINE"), -1);
  }
}
