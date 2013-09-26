package org.apache.helix.api;

import org.apache.helix.api.id.ProcId;
import org.apache.helix.api.id.SessionId;

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
 * A running attributes of a helix instance
 */
public class RunningInstance {
  private final SessionId _sessionId;
  private final HelixVersion _version;
  private final ProcId _pid;

  /**
   * Construct running instance
   * @param sessionId zookeeper session-id
   * @param version helix-version
   * @param pid running jvm name
   */
  public RunningInstance(SessionId sessionId, HelixVersion version, ProcId pid) {
    _sessionId = sessionId;
    _version = version;
    _pid = pid;
  }

  /**
   * Get session id of the running instance
   * session id is the zookeeper session id
   * @return session id
   */
  public SessionId getSessionId() {
    return _sessionId;
  }

  /**
   * Get helix version of the running instance
   * @return helix version
   */
  public HelixVersion getVersion() {
    return _version;
  }

  /**
   * Get the name of the running jvm of the running instance
   * @return running jvm name (e.g. 1111@host)
   */
  public ProcId getPid() {
    return _pid;
  }
}
