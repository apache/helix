package org.apache.helix.participant;

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

import java.util.List;

import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class GenericLeaderStandbyStateModelFactory extends
    StateModelFactory<GenericLeaderStandbyModel> {
  private final CustomCodeCallbackHandler _callback;
  private final List<ChangeType> _notificationTypes;

  public GenericLeaderStandbyStateModelFactory(CustomCodeCallbackHandler callback,
      List<ChangeType> notificationTypes) {
    if (callback == null || notificationTypes == null || notificationTypes.size() == 0) {
      throw new IllegalArgumentException("Require: callback | notificationTypes");
    }
    _callback = callback;
    _notificationTypes = notificationTypes;
  }

  @Override
  public GenericLeaderStandbyModel createNewStateModel(String resourceName, String partitionKey) {
    return new GenericLeaderStandbyModel(_callback, _notificationTypes, partitionKey);
  }
}
