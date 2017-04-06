package org.apache.helix.rest.server;


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

import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;

public class ServerContext {
  private String _zkAddr;
  private ZkClient _zkClient;

  public ServerContext(String zkAddr) {
    _zkAddr = zkAddr;
    _zkClient =
        new ZkClient(_zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
  }

  public ZkClient getZkClient() {
    if (_zkClient == null) {
      _zkClient =
          new ZkClient(_zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    }

    return _zkClient;
  }

  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
    }
  }
}
