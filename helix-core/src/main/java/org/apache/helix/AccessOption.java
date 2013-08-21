package org.apache.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.zookeeper.CreateMode;

public class AccessOption {
  public static int PERSISTENT = 0x1;
  public static int EPHEMERAL = 0x2;
  public static int PERSISTENT_SEQUENTIAL = 0x4;
  public static int EPHEMERAL_SEQUENTIAL = 0x8;
  public static int THROW_EXCEPTION_IFNOTEXIST = 0x10;

  /**
   * Helper method to get zookeeper create mode from options
   * @param options bitmask representing mode; least significant set flag is selected
   * @return zookeeper create mode
   */
  public static CreateMode getMode(int options) {
    if ((options & PERSISTENT) > 0) {
      return CreateMode.PERSISTENT;
    } else if ((options & EPHEMERAL) > 0) {
      return CreateMode.EPHEMERAL;
    } else if ((options & PERSISTENT_SEQUENTIAL) > 0) {
      return CreateMode.PERSISTENT_SEQUENTIAL;
    } else if ((options & EPHEMERAL_SEQUENTIAL) > 0) {
      return CreateMode.EPHEMERAL_SEQUENTIAL;
    }

    return null;
  }

  /**
   * Helper method to get is-throw-exception-on-node-not-exist from options
   * @param options bitmask containing Zookeeper mode options
   * @return true if in is-throw-exception-on-node-not-exist, false otherwise
   */
  public static boolean isThrowExceptionIfNotExist(int options) {
    return (options & THROW_EXCEPTION_IFNOTEXIST) > 0;
  }

}
