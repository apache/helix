package org.apache.helix.rest.server;

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

import org.apache.helix.HelixException;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.resources.ClusterAccessor;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelixRestServer extends ResourceConfig {
  private static Logger LOG = LoggerFactory.getLogger(HelixRestServer.class);

  private int _port;
  private String _urlPrefix;
  private Server _server;

  public HelixRestServer(String zkAddr, int port, String urlPrefix) {
    super(ClusterAccessor.class);
    _port = port;
    _urlPrefix = urlPrefix;

    packages("org.apache.helix.rest.server.resources");
    ServerContext serverContext = new ServerContext(zkAddr);
    property(ContextPropertyKeys.SERVER_CONTEXT.name(), serverContext);

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override public void run() {
        shutdown();
      }
    }));
  }

  public void start() throws HelixException, InterruptedException {
    _server = new Server(_port);
    ServletHolder servlet = new ServletHolder(new ServletContainer(this));
    ServletContextHandler contextHandler = new ServletContextHandler(_server, _urlPrefix);
    contextHandler.addServlet(servlet, "/*");

    try {
      _server.start();
      _server.join();
    } catch (Exception ex) {
      LOG.error("Failed to start Helix rest server, " + ex);
      throw new HelixException("Failed to start Helix rest server! " + ex);
    } finally {
      shutdown();
    }
  }

  private void shutdown() {
    if (_server != null) {
      try {
        _server.stop();
      } catch (Exception ex) {
        LOG.error("Failed to stop Helix rest server, " + ex);
      }
    }
    ServerContext serverContext =
        (ServerContext) getProperty(ContextPropertyKeys.SERVER_CONTEXT.name());
    serverContext.close();
  }
}
