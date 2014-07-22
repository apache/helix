package org.apache.helix.provisioning.participant;

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

import org.apache.helix.HelixConnection;
import org.apache.helix.api.Resource;
import org.apache.helix.api.Scope;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.participant.AbstractParticipantService;
import org.apache.helix.provisioning.ServiceConfig;
import org.apache.log4j.Logger;

public abstract class StatelessParticipantService extends AbstractParticipantService {
  private static final Logger LOG = Logger.getLogger(StatelessParticipantService.class);

  private final String _serviceName;

  public StatelessParticipantService(HelixConnection connection, ClusterId clusterId,
      ParticipantId participantId, String serviceName) {
    super(connection, clusterId, participantId);
    _serviceName = serviceName;
  }

  @Override
  protected void init() {
    ClusterId clusterId = getClusterId();
    ClusterAccessor clusterAccessor = getConnection().createClusterAccessor(clusterId);
    ResourceId resourceId = ResourceId.from(_serviceName);
    Resource resource = clusterAccessor.readResource(resourceId);
    UserConfig userConfig = resource.getUserConfig();
    ServiceConfig serviceConfig = new ServiceConfig(Scope.resource(resourceId));
    serviceConfig.setSimpleFields(userConfig.getSimpleFields());
    serviceConfig.setListFields(userConfig.getListFields());
    serviceConfig.setMapFields(userConfig.getMapFields());
    LOG.info("Starting service:" + _serviceName + " with configuration:" + serviceConfig);
    StatelessServiceStateModelFactory stateModelFactory =
        new StatelessServiceStateModelFactory(this);
    getParticipant().getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.from("StatelessService"), stateModelFactory);
    init(serviceConfig);
  }

  /**
   * Get the name of this stateless service
   * @return service name
   */
  public String getName() {
    return _serviceName;
  }

  /**
   * Initialize the service with a configuration
   */
  protected abstract void init(ServiceConfig serviceConfig);

  /**
   * Invoked when this service is instructed to go online
   */
  protected abstract void goOnline();

  /**
   * Invoked when this service is instructed to go offline
   */
  protected abstract void goOffine();

}
