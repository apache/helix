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

import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;

public class ParticipantAccessor {
  private final HelixDataAccessor _accessor;

  public ParticipantAccessor(HelixDataAccessor accessor) {
    _accessor = accessor;
  }

  /**
   * 
   */
  public void disable(ParticipantId participantId) {

  }

  /**
   * 
   */
  public void enable(ParticipantId participantId) {

  }

  /**
   * @param msgs
   */
  public void insertMsgs(ParticipantId participantId, SessionId sessionId, Map<MsgId, Msg> msgs) {

  }

  /**
   * @param msgs
   */
  public void updateMsgs(ParticipantId participantId, SessionId sessionId, Map<MsgId, Msg> msgs) {

  }

  /**
   * @param msgIdSet
   */
  public void deleteMsgs(ParticipantId participantId, SessionId sessionId, Set<MsgId> msgIdSet) {

  }

  /**
   * @param disablePartitionSet
   */
  public void disablePartitions(ParticipantId participantId, Set<PartitionId> disablePartitionSet) {

  }

  /**
   * @param enablePartitionSet
   */
  public void enablePartitions(ParticipantId participantId, Set<PartitionId> enablePartitionSet) {

  }

  /**
   * create live instance for the participant
   * @param participantId
   */
  public void start(ParticipantId participantId) {

  }

  /**
   * @param participantId
   * @return
   */
  public Participant read(ParticipantId participantId) {
    return null;
  }
}
