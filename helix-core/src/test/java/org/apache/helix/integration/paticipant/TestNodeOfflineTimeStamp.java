package org.apache.helix.integration.paticipant;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ParticipantHistory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestNodeOfflineTimeStamp extends ZkStandAloneCMTestBase {
  final String className = getShortClassName();

  @Test
  public void testNodeShutdown() throws Exception {
    for (MockParticipantManager participant : _participants) {
      ParticipantHistory history = getInstanceHistory(participant.getInstanceName());
      Assert.assertNotNull(history);
      Assert.assertEquals(history.getLastOfflineTime(), ParticipantHistory.ONLINE);
    }

    long shutdownTime = System.currentTimeMillis();
    _participants[0].syncStop();
    ParticipantHistory history = getInstanceHistory(_participants[0].getInstanceName());
    long recordTime = history.getLastOfflineTime();

    Assert.assertTrue(Math.abs(shutdownTime - recordTime) <= 500L);

    _participants[0].reset();
    _participants[0].syncStart();

    history = getInstanceHistory(_participants[0].getInstanceName());
    Assert.assertEquals(history.getLastOfflineTime(), ParticipantHistory.ONLINE);
  }

  private ParticipantHistory getInstanceHistory(String instance) {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey propertyKey = accessor.keyBuilder().participantHistory(instance);
    return accessor.getProperty(propertyKey);
  }
}
