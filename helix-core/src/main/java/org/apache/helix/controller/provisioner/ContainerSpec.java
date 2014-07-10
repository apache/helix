package org.apache.helix.controller.provisioner;

import org.apache.helix.api.id.ParticipantId;

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

public class ContainerSpec {
  /**
   * Some unique id representing the container.
   */
  ContainerId _containerId;

  int _memory;

  private ParticipantId _participantId;

  public ContainerSpec(ParticipantId _participantId) {
    this._participantId = _participantId;
  }

  public ContainerId getContainerId() {
    return _containerId;
  }

  @Override
  public String toString() {
    return _participantId.toString();
  }

  public void setMemory(int memory) {
    _memory = memory;
  }

  public int getMemory() {
    return _memory;
  }

  public static ContainerSpec from(String serialized) {
    // todo
    return null;
    // return new ContainerSpec(ContainerId.from(serialized));
  }

  public ParticipantId getParticipantId() {
    return _participantId;
  }

}
