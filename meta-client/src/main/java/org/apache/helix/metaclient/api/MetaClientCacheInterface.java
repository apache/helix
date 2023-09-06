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
public interface MetaClientCacheInterface<T> extends MetaClientInterface<T> {
    enum  CacheState {
        /**
         * When cache is closed or recently created and hasn't been started yet.
         */
        UNINITIALIZED,
        /**
         * When a change happens in the underlying metadata storage, the
         * cache must be updated accordingly. In this state, the cache is
         * being rebuilt based on the changes that the listener picks up.
         */
        PROCESSING_CHANGES,
        /**
         * The cache is up-to-date with the key from the underlying
         * metadata storage and no changes are being processed.
         */
        UP_TO_DATE
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     * Whether the cache is populated immediately or not is defined when crating the cache client.
     */
    void startCache();

    /**
     * Close / end the cache.
     */
    void closeCache();

    /**
     * Rebuild the entries that exist in the current cache. Will log an error and return null
     * if the cache is close / hasn't started yet.
     */
    void rebuildCache();
}