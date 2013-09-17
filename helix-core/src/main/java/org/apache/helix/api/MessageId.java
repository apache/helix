package org.apache.helix.api;

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

public class MessageId extends Id {

  private final String _id;

  private MessageId(String id) {
    _id = id;
  }

  @Override
  public String stringify() {
    return _id;
  }

  /**
   * Get a concrete message id
   * @param messageId string message identifier
   * @return MsgId
   */
  public static MessageId from(String messageId) {
    if (messageId == null) {
      return null;
    }
    return new MessageId(messageId);
  }
}
