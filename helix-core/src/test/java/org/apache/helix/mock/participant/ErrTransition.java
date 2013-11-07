package org.apache.helix.mock.participant;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.State;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.Message;

// simulate error transition
public class ErrTransition extends MockTransition {
  private final Map<String, Set<String>> _errPartitions;

  public ErrTransition(Map<String, Set<String>> errPartitions) {
    if (errPartitions != null) {
      // change key to upper case
      _errPartitions = new HashMap<String, Set<String>>();
      for (String key : errPartitions.keySet()) {
        String upperKey = key.toUpperCase();
        _errPartitions.put(upperKey, errPartitions.get(key));
      }
    } else {
      _errPartitions = Collections.emptyMap();
    }
  }

  @Override
  public void doTransition(Message message, NotificationContext context) {
    State fromState = message.getTypedFromState();
    State toState = message.getTypedToState();
    PartitionId partition = message.getPartitionId();

    String key = (fromState + "-" + toState).toUpperCase();
    if (_errPartitions.containsKey(key) && _errPartitions.get(key).contains(partition.stringify())) {
      String errMsg =
          "IGNORABLE: test throw exception in msgId: " + message.getId() + " for " + partition
              + " transit from " + fromState + " to " + toState;
      throw new RuntimeException(errMsg);
    }
  }
}
