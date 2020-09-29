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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.ParticipantHistory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestInstanceHistory extends ZkStandAloneCMTestBase {

  @Test()
  public void testInstanceHistory() throws Exception {
    HelixManager manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    manager.connect();

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);
    PropertyKey propertyKey = keyBuilder.participantHistory(_participants[0].getInstanceName());
    ParticipantHistory history = manager.getHelixDataAccessor().getProperty(propertyKey);
    Assert.assertNotNull(history);
    List<String> list = history.getRecord().getListField("HISTORY");
    Assert.assertEquals(list.size(), 1);

    Assert.assertTrue(list.get(0).contains("SESSION=" + _participants[0].getSessionId()));
    Assert.assertTrue(list.get(0).contains("VERSION=" + _participants[0].getVersion()));

    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
      hostname = "UnknownHostname";
    }
    Assert
        .assertTrue(list.get(0).contains("HOST=" + hostname));

    Assert.assertTrue(list.get(0).contains("TIME="));
    Assert.assertTrue(list.get(0).contains("DATE="));

    for (int i = 0; i <= 22; i++) {
      _participants[0].disconnect();
      _participants[0].connect();
    }

    history = manager.getHelixDataAccessor().getProperty(propertyKey);
    Assert.assertNotNull(history);
    list = history.getRecord().getListField("HISTORY");
    Assert.assertEquals(list.size(), 20);
    list = history.getRecord().getListField("OFFLINE");
    Assert.assertEquals(list.size(), 20);
    manager.disconnect();
  }
}
