package org.apache.helix.manager.zk;

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

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test {@link org.apache.helix.HelixManagerFactory}. MSDS usages are tested in
 * {@link org.apache.helix.integration.multizk.TestMultiZkHelixJavaApis}
 */
public class TestHelixManagerFactory extends ZkStandAloneCMTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestHelixManagerFactory.class);
  private static final String PARTICIPANT_NAME_PREFIX = "testInstance_";
  private HelixAdmin _helixAdmin;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _helixAdmin = new ZKHelixAdmin.Builder().setZkAddress(ZK_ADDR).build();
  }

  @Test
  public void testZKConnectionConfigForNonMsdsConnection() throws Exception {
    // Create participant
    String participantName = PARTICIPANT_NAME_PREFIX + "testZKConnectionConfigForNonMsdsConnection";
    _helixAdmin.addInstance(CLUSTER_NAME, new InstanceConfig(participantName));

    // Create zk helix manager using HelixManagerFactory API
    HelixManager managerParticipant = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, participantName, InstanceType.PARTICIPANT, null,
            new HelixManagerProperty.Builder()
                .setZkConnectionConfig(new HelixZkClient.ZkConnectionConfig(ZK_ADDR)).build());
    managerParticipant.connect();

    // Verify manager
    InstanceConfig instanceConfigRead =
        _helixAdmin.getInstanceConfig(CLUSTER_NAME, participantName);
    Assert.assertNotNull(instanceConfigRead);
    Assert.assertEquals(instanceConfigRead.getInstanceName(), participantName);
  }
}
