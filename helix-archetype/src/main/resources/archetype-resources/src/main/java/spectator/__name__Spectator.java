package ${package}.spectator;

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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.spectator.RoutingTableProvider;

public class ${name}Spectator {
  private final String zkConnectString;
  private final ClusterId clusterId;
  private final ParticipantId participantId;

  private HelixManager manager;
  private RoutingTableProvider routingTableProvider;

  public ${name}Spectator(String zkConnectString, ClusterId clusterId, ParticipantId participantId) {
    this.zkConnectString = zkConnectString;
    this.clusterId = clusterId;
    this.participantId = participantId;
  }

  public void start() {
    try {
      manager = HelixManagerFactory.getZKHelixManager(
          clusterId.stringify(),
          participantId.stringify(),
          InstanceType.SPECTATOR,
          zkConnectString);
      manager.connect();
      routingTableProvider = new RoutingTableProvider();
      manager.addExternalViewChangeListener(routingTableProvider);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public void stop() {
    manager.disconnect();
  }

  public RoutingTableProvider getRoutingTableProvider() {
    return routingTableProvider;
  }
}
