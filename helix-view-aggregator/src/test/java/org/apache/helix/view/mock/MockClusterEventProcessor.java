package org.apache.helix.view.mock;

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

import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.view.common.ClusterViewEvent;

public class MockClusterEventProcessor
    extends DedupEventProcessor<ClusterViewEvent.Type, ClusterViewEvent> {
  private int _handledClusterConfigChange;
  private int _handledExternalViewChange;
  private int _handledInstanceConfigChange;
  private int _handledLiveInstancesChange;

  public MockClusterEventProcessor(String clusterName) {
    super(clusterName);
    resetHandledEventCount();
  }

  public int getHandledClusterConfigChangeCount() {
    return _handledClusterConfigChange;
  }

  public int getHandledExternalViewChangeCount() {
    return _handledExternalViewChange;
  }

  public int getHandledInstanceConfigChangeCount() {
    return _handledInstanceConfigChange;
  }

  public int getHandledLiveInstancesChangeCount() {
    return _handledLiveInstancesChange;
  }

  public void resetHandledEventCount() {
    _handledClusterConfigChange = 0;
    _handledExternalViewChange = 0;
    _handledInstanceConfigChange = 0;
    _handledLiveInstancesChange = 0;
  }

  @Override
  public void handleEvent(ClusterViewEvent event) {
    switch (event.getEventType()) {
    case ConfigChange:
      _handledClusterConfigChange += 1;
      break;
    case LiveInstanceChange:
      _handledLiveInstancesChange += 1;
      break;
    case InstanceConfigChange:
      _handledInstanceConfigChange += 1;
      break;
    case ExternalViewChange:
      _handledExternalViewChange += 1;
      break;
    default:
      break;
    }
  }
}
