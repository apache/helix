package org.apache.helix.metaclient.policy;

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

import org.apache.helix.metaclient.policy.MetaClientReconnectPolicy;


/**
 * Policy to define client re-establish connection behavior when connection to underlying metadata
 * store is expired.
 * If this retry policy is passed to MetaClient, no auto retry connection will be issued when
 * connection lost or expired.
 */
public class NoRetryReconnectPolicy implements MetaClientReconnectPolicy {
  @Override
  public RetryPolicyName getPolicyName() {
    return RetryPolicyName.NO_RETRY;
  }
}
