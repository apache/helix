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
 * An asynchronous callback is deferred to invoke after an async CRUD operation finish and return.
 * The corresponding callback is registered when async CRUD API is invoked. Implementation processes
 * the result of each CRUD call. It should check return code and perform accordingly.
 */
// TODO: define return code. failure code should map to MetaClient exceptions.
public interface AsyncCallback {
  //This callback is used when stat object is returned from the operation.
  interface StatCallback extends AsyncCallback {
    /**
     * Process the result of asynchronous calls that returns a stat object.
     * @param returnCode  The return code of the call.
     * @param key the key that passed to asynchronous calls.
     * @param context context object that passed to asynchronous calls.
     * @param stat the stats of the entry of the given key, returned from the async call.
     */
    void processResult(int returnCode, String key, Object context, MetaClientInterface.Stat stat);
  }

  //This callback is used when data is returned from the operation.
  interface DataCallback extends AsyncCallback {
    /**
     * Process the result of asynchronous calls that returns entry data.
     * @param returnCode  The return code of the call.
     * @param key The key that passed to asynchronous calls.
     * @param context context object that passed to asynchronous calls.
     * @param data returned entry data from the call.
     * @param stat the stats of the entry of the given key.
     */
    void processResult(int returnCode, String key, Object context, byte[] data, MetaClientInterface.Stat stat);
  }

  //This callback is used when nothing is returned from the operation.
  interface VoidCallback extends AsyncCallback {
    /**
     * Process the result of asynchronous calls that has no return value.
     * @param returnCode  The return code of the call.
     * @param key he key that passed to asynchronous calls.
     * @param context context object that passed to asynchronous calls.
     */
    void processResult(int returnCode, String key, Object context);
  }

  //This callback is used to process the list if OpResults from a single transactional call.
  interface TransactionCallback extends AsyncCallback {
    /**
     * Process the result of asynchronous transactional calls.
     * @param returnCode  The return code of the transaction call.
     * @param keys List of keys passed to the async transactional call.
     * @param context context object that passed to asynchronous calls.
     * @param opResults The list of transactional results.
     */
    void processResult(int returnCode, List<String> keys, Object context, List<OpResult> opResults);
  }

}