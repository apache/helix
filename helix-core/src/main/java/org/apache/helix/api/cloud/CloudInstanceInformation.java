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
 * Generic interface for cloud instance information
 */
public interface CloudInstanceInformation {
  /**
   * Get the the value of a specific cloud instance field by name
   * @return the value of the field
   */
  String get(String key);

  /**
   * The enum contains all the required cloud instance field in Helix
   */
  enum CloudInstanceField {
    INSTANCE_NAME,
    FAULT_DOMAIN,
    INSTANCE_SET_NAME
  }
}
