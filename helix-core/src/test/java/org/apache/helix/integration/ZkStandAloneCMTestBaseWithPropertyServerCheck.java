package org.apache.helix.integration;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.restlet.ZKPropertyTransferServer;
import org.apache.helix.controller.restlet.ZkPropertyTransferClient;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.StatusUpdate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * setup a storage cluster and start a zk-based cluster controller in stand-alone mode
 * start 5 dummy participants verify the current states at end
 */

public class ZkStandAloneCMTestBaseWithPropertyServerCheck extends ZkStandAloneCMTestBase {
  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    ZKPropertyTransferServer.PERIOD = 500;
    ZkPropertyTransferClient.SEND_PERIOD = 500;
    ZKPropertyTransferServer.getInstance().init(19999, ZK_ADDR);
    super.beforeClass();

    Thread.sleep(1000);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder kb = accessor.keyBuilder();

    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = _participants[i].getInstanceName();
      List<StatusUpdate> statusUpdates =
          accessor.getChildValues(kb.stateTransitionStatus(instanceName,
              _participants[i].getSessionId(), TEST_DB));

        for (int j = 0; j < 10; j++) {
          statusUpdates =
              accessor.getChildValues(kb.stateTransitionStatus(instanceName,
                _participants[i].getSessionId(), TEST_DB));
          if (statusUpdates.size() == 0) {
            Thread.sleep(500);
          } else {
            break;
          }
        }
        Assert.assertTrue(statusUpdates.size() > 0);
        for (StatusUpdate update : statusUpdates) {
          Assert.assertTrue(update.getRecord()
              .getSimpleField(ZkPropertyTransferClient.USE_PROPERTYTRANSFER).equals("true"));
          Assert
              .assertTrue(update.getRecord().getSimpleField(ZKPropertyTransferServer.SERVER) != null);
        }
    }
  }

  @Override
  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
    ZKPropertyTransferServer.getInstance().shutdown();
    ZKPropertyTransferServer.getInstance().reset();
  }
}
