package org.apache.helix.zookeeper.zkclient;

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

/**
 * An interface representing data being written to ZK is session aware:
 * data is supposed to be written by expected ZK session. If ZkClient's actual session
 * doesn't match expected session, data is not written to ZK.
 */
public interface SessionAwareZkWriteData {
  /**
   * Gets id of expected session that is supposed to write the data.
   *
   * @return expected zk session id
   */
  String getExpectedSessionId();
}
