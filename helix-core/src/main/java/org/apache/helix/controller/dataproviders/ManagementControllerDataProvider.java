package org.apache.helix.controller.dataproviders;

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

import java.util.Arrays;
import java.util.List;

import org.apache.helix.HelixConstants;

/**
 * Data provider for controller management mode pipeline.
 */
public class ManagementControllerDataProvider extends BaseControllerDataProvider {
  // Only these types of properties are refreshed for the full refresh request.
  private static final List<HelixConstants.ChangeType> FULL_REFRESH_PROPERTIES =
      Arrays.asList(HelixConstants.ChangeType.LIVE_INSTANCE, HelixConstants.ChangeType.MESSAGE);

  public ManagementControllerDataProvider(String clusterName, String pipelineName) {
    super(clusterName, pipelineName);
  }

  @Override
  public void requireFullRefresh() {
    for (HelixConstants.ChangeType type : FULL_REFRESH_PROPERTIES) {
      _propertyDataChangedMap.get(type).set(true);
    }
  }
}
