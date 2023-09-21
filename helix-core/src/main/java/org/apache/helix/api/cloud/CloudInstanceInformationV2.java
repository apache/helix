package org.apache.helix.api.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.ImmutableMap;

/**
 * Generic interface for cloud instance information which builds on top of CloudInstanceInformation.
 * This interface adds a new method, getAll(), which returns all the key value pairs of a specific cloud instance.
 * We call suffix the name of this interface with V2 to preserve backwards compatibility for all classes
 * that implement CloudInstanceInformation.
 */
public interface CloudInstanceInformationV2 extends CloudInstanceInformation {
  /**
   * Get all the key value pairs of a specific cloud instance
   * @return A map of all the key value pairs
   */
  ImmutableMap<String, String> getAll();
}
