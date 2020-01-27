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

package org.apache.helix.lock;

import java.util.Map;


/**
 * Generic interface for a map contains the Helix lock information
 * @param <T> The type of the LockInfo value
 */
public interface LockInfo<T> {

  /**
   * Create a single filed of LockInfo, or update the value of the field if it already exists
   * @param infoKey the key of the LockInfo field
   * @param infoValue the value of the LockInfo field
   */
  void setInfoValue(String infoKey, T infoValue);

  /**
   * Get the value of a field in LockInfo
   * @param infoKey the key of the LockInfo field
   * @return the value of the field or null if this key does not exist
   */
  T getInfoValue(String infoKey);
}
