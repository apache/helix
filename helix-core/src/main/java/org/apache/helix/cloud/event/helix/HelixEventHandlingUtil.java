package org.apache.helix.cloud.event.helix;

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

import org.apache.helix.BaseDataAccessor;


 class HelixEventHandlingUtil {

  /**
   * Enable or disable an instance for cloud event.
   * It will enable/disable Helix for that instance. Also add the instance cloud event info to
   * clusterConfig Znode when enable.
   * @param clusterName
   * @param instanceName
   * @param message
   * @param isEnable
   * @param dataAccessor
   * @return return failure when either enable/disable failed or update cluster ZNode failed.
   */
   static boolean enableInstanceForCloudEvent(String clusterName, String instanceName, String message,
      boolean isEnable, BaseDataAccessor dataAccessor) {
    // TODO add impl here
    return true;
  }

  /**
   * check if instance is disabled by cloud event.
   * @param clusterName
   * @param instanceName
   * @param dataAccessor
   * @return return true only when instance is Helix disabled and has the cloud event info in
   * clusterConfig ZNode.
   */
   static boolean IsInstanceDisabledForCloudEvent(String clusterName, String instanceName,
      BaseDataAccessor dataAccessor) {
    // TODO add impl here
    return true;
  }

}
