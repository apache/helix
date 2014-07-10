package org.apache.helix.provisioning.yarn.example;

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
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.participant.AbstractParticipantService;
import org.apache.helix.provisioning.ServiceConfig;
import org.apache.helix.provisioning.participant.StatelessParticipantService;
import org.apache.log4j.Logger;

public class HelloWorldService extends StatelessParticipantService {

  private static Logger LOG = Logger.getLogger(AbstractParticipantService.class);

  static String SERVICE_NAME = "HelloWorld";

  public HelloWorldService(HelixConnection connection, ClusterId clusterId,
      ParticipantId participantId) {
    super(connection, clusterId, participantId, SERVICE_NAME);
  }

  @Override
  protected void init(ServiceConfig serviceConfig) {
    LOG.info("Initialized service with config " + serviceConfig);
  }

  @Override
  protected void goOnline() {
    LOG.info("HelloWorld service is told to go online");
  }

  @Override
  protected void goOffine() {
    LOG.info("HelloWorld service is told to go offline");
  }

}
