package org.apache.helix;

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
 * Called to allow definition of tasks prior to connecting to Zookeeper
 */
public interface PreConnectCallback {
  /**
   * Callback function that is called by HelixManager before connected to zookeeper. If
   * exception are thrown HelixManager will not connect and no live instance is created
   * @see ZkHelixManager#handleNewSessionAsParticipant()
   */
  public void onPreConnect();
}
