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

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.Message;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

// simulate access property store and update one znode
public class StoreAccessOneNodeTransition extends MockTransition {
  @Override
  public void doTransition(Message message, NotificationContext context) {
    HelixManager manager = context.getManager();
    ZkHelixPropertyStore<ZNRecord> store = manager.getHelixPropertyStore();
    final String setPath = "/TEST_PERF/set";
    final String updatePath = "/TEST_PERF/update";
    final PartitionId key = message.getPartitionId();
    try {
      // get/set once
      ZNRecord record = null;
      try {
        record = store.get(setPath, null, 0);
      } catch (ZkNoNodeException e) {
        record = new ZNRecord(setPath);
      }
      record.setSimpleField("setTimestamp", "" + System.currentTimeMillis());
      store.set(setPath, record, AccessOption.PERSISTENT);

      // update once
      store.update(updatePath, new DataUpdater<ZNRecord>() {

        @Override
        public ZNRecord update(ZNRecord currentData) {
          if (currentData == null) {
            currentData = new ZNRecord(updatePath);
          }
          currentData.setSimpleField(key.stringify(), "" + System.currentTimeMillis());

          return currentData;
        }

      }, AccessOption.PERSISTENT);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
}
