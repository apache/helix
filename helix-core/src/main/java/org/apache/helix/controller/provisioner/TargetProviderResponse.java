package org.apache.helix.controller.provisioner;

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

import java.util.List;

import org.apache.helix.api.Participant;

public class TargetProviderResponse {

  List<ContainerSpec> containersToAcquire;

  List<Participant> containersToRelease;

  List<Participant> containersToStop;

  List<Participant> containersToStart;

  public List<ContainerSpec> getContainersToAcquire() {
    return containersToAcquire;
  }

  public void setContainersToAcquire(List<ContainerSpec> containersToAcquire) {
    this.containersToAcquire = containersToAcquire;
  }

  public List<Participant> getContainersToRelease() {
    return containersToRelease;
  }

  public void setContainersToRelease(List<Participant> containersToRelease) {
    this.containersToRelease = containersToRelease;
  }

  public List<Participant> getContainersToStop() {
    return containersToStop;
  }

  public void setContainersToStop(List<Participant> containersToStop) {
    this.containersToStop = containersToStop;
  }

  public List<Participant> getContainersToStart() {
    return containersToStart;
  }

  public void setContainersToStart(List<Participant> containersToStart) {
    this.containersToStart = containersToStart;
  }

}
