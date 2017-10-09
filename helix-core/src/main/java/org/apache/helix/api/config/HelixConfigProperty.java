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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;

/**
 * This class defines all possible configuration options
 * and its applicable config scopes (eg, Cluster/Resource/Instance/Partition config).
 */
public enum HelixConfigProperty {
  P2P_MESSAGE_ENABLED(ConfigScopeProperty.CLUSTER, ConfigScopeProperty.RESOURCE);

  Set<ConfigScopeProperty> _applicableScopes;

  HelixConfigProperty(ConfigScopeProperty ...configScopeProperties) {
    _applicableScopes = new HashSet<>(Arrays.asList(configScopeProperties));
  }

  public Set<ConfigScopeProperty> applicableScopes() {
    return _applicableScopes;
  }
}
