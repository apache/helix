package org.apache.helix.webapp;

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

import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;

public class HelixAdminWebApp {
  public final Logger LOG = Logger.getLogger(HelixAdminWebApp.class);
  RestAdminApplication _adminApp = null;
  Component _component = null;

  int _helixAdminPort;
  String _zkServerAddress;
  ZkClient _zkClient;

  public HelixAdminWebApp(String zkServerAddress, int adminPort) {
    _zkServerAddress = zkServerAddress;
    _helixAdminPort = adminPort;
  }

  public synchronized void start() throws Exception {
    LOG.info("helixAdminWebApp starting");
    if (_component == null) {
      _zkClient =
          new ZkClient(_zkServerAddress, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      _component = new Component();
      _component.getServers().add(Protocol.HTTP, _helixAdminPort);
      Context applicationContext = _component.getContext().createChildContext();
      applicationContext.getAttributes()
          .put(RestAdminApplication.ZKSERVERADDRESS, _zkServerAddress);
      applicationContext.getAttributes().put(RestAdminApplication.PORT, "" + _helixAdminPort);
      applicationContext.getAttributes().put(RestAdminApplication.ZKCLIENT, _zkClient);
      _adminApp = new RestAdminApplication(applicationContext);
      // Attach the application to the component and start it
      _component.getDefaultHost().attach(_adminApp);
      _component.start();
    }
    LOG.info("helixAdminWebApp started on port " + _helixAdminPort);
  }

  public synchronized void stop() {
    try {
      _component.stop();
    } catch (Exception e) {
      LOG.error("", e);
    } finally {
      if (_zkClient != null) {
        _zkClient.close();
      }
    }
  }
}
