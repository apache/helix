package org.apache.helix.controller.stages;

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

/**
 * A record entry in cluster data cache containing information about a partition's
 * missing top state
 */
public class MissingTopStateRecord {
  private final boolean isGracefulHandoff;
  private final long startTimeStamp;
  private final long userLatency;
  private boolean failed;

  public MissingTopStateRecord(long start, long user, boolean graceful) {
    isGracefulHandoff = graceful;
    startTimeStamp = start;
    userLatency = user;
    failed = false;
  }

  /* package */ boolean isGracefulHandoff() {
    return isGracefulHandoff;
  }

  /* package */ long getStartTimeStamp() {
    return startTimeStamp;
  }

  /* package */ long getUserLatency() {
    return userLatency;
  }

  /* package */ void setFailed() {
    failed = true;
  }

  /* package */ boolean isFailed() {
    return failed;
  }
}
