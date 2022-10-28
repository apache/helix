package org.apache.helix.zookeeper.impl.client;

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

import java.io.IOException;

import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestSharedZkClient extends RealmAwareZkClientFactoryTestBase {

  @BeforeClass
  public void beforeClass() throws IOException, InvalidRoutingDataException {
    super.beforeClass();
    // Set the factory to SharedZkClientFactory
    _realmAwareZkClientFactory = SharedZkClientFactory.getInstance();
  }

  @Test(dependsOnMethods = "testRealmAwareZkClientCreation")
  public void testCreateEphemeralFailure() {
    _realmAwareZkClient.setZkSerializer(new ZNRecordSerializer());

    // Create a dummy ZNRecord
    ZNRecord znRecord = new ZNRecord("DummyRecord");
    znRecord.setSimpleField("Dummy", "Value");

    // test createEphemeral should fail
    try {
      _realmAwareZkClient.createEphemeral(TEST_VALID_PATH);
      Assert.fail(
          "sharedReamlAwareZkClient is not expected to be able to create ephemeral node via createEphemeral");
    } catch (UnsupportedOperationException e) {
      // this is expected
    }

    // test creating Ephemeral via creat would also fail
    try {
      _realmAwareZkClient.create(TEST_VALID_PATH, znRecord, CreateMode.EPHEMERAL);
      Assert.fail(
          "sharedRealmAwareZkClient is not expected to be able to create ephmeral node via create");
    } catch (UnsupportedOperationException e) {
      // this is expected.
    }
  }
}