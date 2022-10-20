package org.apache.helix.metaclient.api;

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

import java.util.List;

/**
 * An asynchronous callback is deferred to invoke after an async CRUD operation returns.
 * The corresponding callback is registered when async CRUD API is invoked.
 */
public interface AsyncCallback {
  //This callback is used when stat object is returned from the operation.
  interface StatCallback extends AsyncCallback {
    void processResult(int returnCode, String key, Object context, MetaClientInterface.Stat stat);
  }

  //This callback is used when data is returned from the operation.
  interface DataCallback extends AsyncCallback {
    void processResult(int returnCode, String key, Object context, byte[] data, MetaClientInterface.Stat stat);
  }

  //This callback is used when nothing is returned from the operation.
  interface VoidCallback extends AsyncCallback {
    void processResult(int returnCode, String key, Object context);
  }

  //This callback is used to process the list if OpResults from a single transactional call.
  interface TransactionCallback extends AsyncCallback {
    void processResult(int returnCode, String key, Object context, List<OpResult> opResults);
  }

}