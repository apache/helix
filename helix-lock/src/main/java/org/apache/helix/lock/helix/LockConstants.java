package org.apache.helix.lock.helix;

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


/*
 * Default values for each attribute if there are no current values set by user
 */
public class LockConstants {
  public static final String DEFAULT_OWNER_TEXT = "NONE";
  public static final String DEFAULT_MESSAGE_TEXT = "NONE";
  public static final long DEFAULT_TIMEOUT_LONG = -1;
  public static final int DEFAULT_PRIORITY_INT = -1;
  public static final long DEFAULT_WAITING_TIMEOUT_LONG = -1;
  public static final long DEFAULT_CLEANUP_TIMEOUT_LONG = -1;
  public static final String DEFAULT_REQUESTOR_ID = "NONE";
  public static final int DEFAULT_REQUESTOR_PRIORITY_INT = -1;
  public static final long DEFAULT_REQUESTOR_WAITING_TIMEOUT_LONG = -1;
  public static final long DEFAULT_REQUESTOR_REQUESTING_TIMESTAMP_LONG = -1;
}
