package org.apache.helix.zookeeper.datamodel;

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

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A class represents a session aware ZNRecord: the ZNRecord should be written to zk by
 * the expected zk session. When the ZNRecord is being written to zk, if the actual
 * zk session id doesn't match the expected zk session id set in the {@code SessionAwareZNRecord},
 * writing to zk will fail. It is supposed to be used within Helix only.
 * <p>
 * If this ZNRecord is not supposed to be written only by the expected zk session,
 * {@link ZNRecord} is recommended to use.
 */
public class SessionAwareZNRecord extends ZNRecord {
  @JsonIgnore
  private String expectedSessionId;

  public SessionAwareZNRecord(String id) {
    super(id);
  }

  public SessionAwareZNRecord(ZNRecord record, String id) {
    super(record, id);
  }

  /**
   * Gets expected zk session id.
   *
   * @return id of the expected zk session that is supposed to write the ZNRecord.
   */
  @JsonIgnore
  public String getExpectedSessionId() {
    return expectedSessionId;
  }

  /**
   * Sets expected zk session id that is supposed to write the ZNRecord.
   *
   * @param sessionId
   */
  @JsonIgnore
  public void setExpectedSessionId(String sessionId) {
    expectedSessionId = sessionId;
  }
}
