package org.apache.helix.customizedstate;

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

import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CustomizedState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for Helix customers to input customized state
 */
public class CustomizedStateProvider {
  private static final Logger LOG = LoggerFactory.getLogger(CustomizedStateProvider.class);
  private final HelixManager _helixManager;
  String _instanceName;

  public CustomizedStateProvider(HelixManager helixManager, String instanceName) {
    _helixManager = helixManager;
    _instanceName = instanceName;
  }

  public synchronized void updateCustomizedState(String customizedStateName,
      Map<String, Map<String, Map<String, String>>> customizedState) {
    for (String resourceName : customizedState.keySet()) {
      ZNRecord record = new ZNRecord(resourceName);
      record.setMapFields(customizedState.get(resourceName));
      HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      if (!accessor.setProperty(
          keyBuilder.customizedState(_instanceName, customizedStateName, record.getId()),
          new CustomizedState(record))) {
        throw new HelixException(String.format(
            "Failed to persist customized state %s to zk for instance %s, resource %s",
            customizedStateName, _instanceName, record.getId()));
      }
    }
  }
}
