package org.apache.helix.common.caches;

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

import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.model.CustomizedState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomizedStateCache extends ParticipantStateCache<CustomizedState> {
  private static final Logger LOG = LoggerFactory.getLogger(CurrentStateCache.class.getName());

  public CustomizedStateCache(String clusterName) {
    this(createDefaultControlContextProvider(clusterName));
  }

  public CustomizedStateCache(ControlContextProvider contextProvider) {
    super(contextProvider);
  }
}