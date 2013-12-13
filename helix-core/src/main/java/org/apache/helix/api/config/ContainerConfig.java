package org.apache.helix.api.config;

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

import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;

public class ContainerConfig {
  private final ContainerId _id;
  private final ContainerSpec _spec;
  private final ContainerState _state;

  /**
   * Instantiate a ContainerConfig
   * @param id the container id
   * @param spec spec of the container
   * @param state state that the container is in
   */
  public ContainerConfig(ContainerId id, ContainerSpec spec, ContainerState state) {
    _id = id;
    _spec = spec;
    _state = state;
  }

  /**
   * Get the container id
   * @return ContainerId
   */
  public ContainerId getId() {
    return _id;
  }

  /**
   * Get the spec for the container
   * @return ContainerSpec
   */
  public ContainerSpec getSpec() {
    return _spec;
  }

  /**
   * Get the state of the container
   * @return ContainerState
   */
  public ContainerState getState() {
    return _state;
  }
}
