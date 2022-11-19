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

/**
 * Listener interface for direct child change event on a particular key. It includes new
 * child creation or child deletion. The callback won't differentiate these types.
 * For hierarchy key spaces like zookeeper, it refers to an entry's direct child nodes.
 * For flat key spaces, it refers to keys that matches `prefix*separator`.
 */
public interface DirectChildChangeListener {
   /**
    * Called when there is a direct child entry creation or deleted.
    * @param key The parent key where child change listener is subscribed. It would be the key
    *            passed to subscribeDirectChildChange.
    * @throws Exception
    */
   void handleDirectChildChange(String key) throws Exception;

}
