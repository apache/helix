package org.apache.helix.api.config;

import org.apache.helix.HelixProperty;
import org.apache.helix.api.Scope;

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
 * Generic user-defined configuration of Helix components
 */
public class UserConfig extends NamespacedConfig {
  /**
   * Instantiate a UserConfig. It is intended for use only by entities that can be identified
   * @param scope scope of the configuration, e.g. cluster, resource, partition, participant, etc
   */
  public UserConfig(Scope<?> scope) {
    super(scope, UserConfig.class.getSimpleName());
  }

  /**
   * Instantiate a UserConfig from an existing HelixProperty
   * @param property property wrapping a configuration
   */
  private UserConfig(HelixProperty property) {
    super(property, UserConfig.class.getSimpleName());
  }

  /**
   * Get a UserConfig that filters out the user-specific configurations in a property
   * @param property the property to extract from
   * @return UserConfig
   */
  public static UserConfig from(HelixProperty property) {
    return new UserConfig(property);
  }
}
