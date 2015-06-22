package org.apache.helix.ui.api;

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

public class InstanceSpec implements Comparable<InstanceSpec> {
  private final String instanceName;
  private final boolean enabled;
  private final boolean live;

  public InstanceSpec(String instanceName,
                      boolean enabled,
                      boolean live) {
    this.instanceName = instanceName;
    this.enabled = enabled;
    this.live = live;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isLive() {
    return live;
  }

  @Override
  public int compareTo(InstanceSpec o) {
    return instanceName.compareTo(o.instanceName);
  }
}
