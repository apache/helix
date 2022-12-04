package org.apache.helix.metaclient.constants;

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

public final class MetaClientConstants {

  private MetaClientConstants(){

  }

  // Stop retrying when we reach timeout
  //TODO The value should be the same as Helix default ZK retry time. Modify when change #2293 merged
  public static final int DEFAULT_OPERATION_RETRY_TIMEOUT_MS = Integer.MAX_VALUE;

  // maxMsToWaitUntilConnected
  public static final int DEFAULT_CONNECTION_INIT_TIMEOUT_MS = 60 * 1000;

  // When a client becomes partitioned from the metadata service for more than session timeout,
  // new session will be established.
  public static final int DEFAULT_SESSION_TIMEOUT_MS = 30 * 1000;



}
