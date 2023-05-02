package org.apache.helix.controller;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.Message;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestCapacityBasedThrottler {

  @Test
  public void basicTest() {
    CapacityBasedThrottler throttler = new CapacityBasedThrottler(
        mockDataProvider(),
        instanceName -> Map.of("DISK", 3, "CU", 4, "COUNT", 5),
        (resource, partition) -> Map.of("DISK", 1, "COUNT", 1));

    Message message = Mockito.mock(Message.class);
    when(message.getResourceName()).thenReturn("testResource");
    when(message.getPartitionName()).thenReturn("testPartition");
    when(message.getFromState()).thenReturn("OFFLINE");
    String instance1 = "instance1";
    String instance2 = "instance2";
    for (int i = 0; i < 3; i++) {
      // DISK is the bottleneck, DISK:3
      Assert.assertTrue(throttler.tryProcessMessage(instance1, message));
    }
    Assert.assertFalse(throttler.tryProcessMessage(instance1, message));
    throttler.addIgnoreKey("DISK");
    // ignore DISK constraint, now the bottleneck is on COUNT:5
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(throttler.tryProcessMessage(instance1, message));
    }
    Map<String, Integer> remainingCopy = new HashMap<>(throttler._instanceRemainingCapacity.get(instance1));
    Assert.assertFalse(throttler.tryProcessMessage(instance1, message));
    // no capacity change if message is throttled
    Assert.assertEquals(remainingCopy, throttler._instanceRemainingCapacity.get(instance1));
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(throttler.tryProcessMessage(instance2, message));
    }
    Assert.assertFalse(throttler.tryProcessMessage(instance2, message));
    // change the message fromState, no throttle applied to non-bootstrapping transitions
    when(message.getFromState()).thenReturn("STANDBY");
    throttler.removeIgnoreKey("DISK");
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(throttler.tryProcessMessage(instance2, message));
    }
  }

  private ResourceControllerDataProvider mockDataProvider() {
    ResourceControllerDataProvider dataCache = Mockito.mock(ResourceControllerDataProvider.class);
    IdealState idealState = Mockito.mock(IdealState.class);
    when(dataCache.getIdealState(any())).thenReturn(idealState);
    when(dataCache.getStateModelDef(any())).thenReturn(new LeaderStandbySMD());
    return dataCache;
  }
}
