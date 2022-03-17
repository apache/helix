package org.apache.helix.cloud.event;

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

import java.util.HashSet;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.cloud.event.helix.DefaultCloudEventCallbackImpl;

public class MockCloudEventCallbackImpl extends DefaultCloudEventCallbackImpl {
  public enum OperationType {
    ON_PAUSE_DISABLE_INSTANCE,
    ON_RESUME_ENABLE_INSTANCE,
    ON_PAUSE_MAINTENANCE_MODE,
    ON_RESUME_MAINTENANCE_MODE,
    PRE_ON_PAUSE,
    POST_ON_PAUSE,
    PRE_ON_RESUME,
    POST_ON_RESUME
  }

  public static Set<OperationType> triggeredOperation = new HashSet<>();

  @Override
  public void disableInstance(HelixManager manager, Object eventInfo) {
    triggeredOperation.add(OperationType.ON_PAUSE_DISABLE_INSTANCE);
  }

  @Override
  public void enableInstance(HelixManager manager, Object eventInfo) {
    triggeredOperation.add(OperationType.ON_RESUME_ENABLE_INSTANCE);
  }

  @Override
  public void enterMaintenanceMode(HelixManager manager, Object eventInfo) {
    triggeredOperation.add(OperationType.ON_PAUSE_MAINTENANCE_MODE);
  }

  @Override
  public void exitMaintenanceMode(HelixManager manager, Object eventInfo) {
    triggeredOperation.add(OperationType.ON_RESUME_MAINTENANCE_MODE);
  }
}
