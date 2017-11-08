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

import java.util.Collections;
import java.util.List;
import org.apache.helix.HelixException;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.auditlog.AuditLogger;
import org.apache.helix.rest.server.filters.AuditLogFilter;
import org.apache.helix.rest.server.filters.CORSFilter;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelixRestServer extends ResourceConfig {
  private static Logger LOG = LoggerFactory.getLogger(HelixRestServer.class);

  private int _port;
  private String _urlPrefix;
  private Server _server;
  private ServerContext _serverContext;

  public HelixRestServer(String zkAddr, int port, String urlPrefix, List<AuditLogger> auditLoggers) {
    _port = port;
    _urlPrefix = urlPrefix;
    _server = new Server(_port);

    packages(AbstractResource.class.getPackage().getName());

    _serverContext = new ServerContext(zkAddr);
    property(ContextPropertyKeys.SERVER_CONTEXT.name(), _serverContext);

    register(new CORSFilter());
    register(new AuditLogFilter(auditLoggers));

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override public void run() {
        shutdown();
      }
    }));
  }

  public HelixRestServer(String zkAddr, int port, String urlPrefix) {
    this(zkAddr, port, urlPrefix, Collections.<AuditLogger>emptyList());
  }

  public void start() throws HelixException, InterruptedException {
    ServletHolder servlet = new ServletHolder(new ServletContainer(this));
    ServletContextHandler contextHandler = new ServletContextHandler(_server, _urlPrefix);
    contextHandler.addServlet(servlet, "/*");

    try {
      _server.start();
    } catch (Exception ex) {
      LOG.error("Failed to start Helix rest server, " + ex);
      throw new HelixException("Failed to start Helix rest server! " + ex);
    }

    LOG.info("Helix rest server started!");
  }

  public void join() {
    if (_server != null) {
      try {
        _server.join();
      } catch (InterruptedException e) {
        LOG.warn("Join on Helix rest server get interrupted!" + e);
      }
    }
  }

  public void shutdown() {
    if (_server != null) {
      try {
        _server.stop();
        LOG.info("Helix rest server stopped!");
      } catch (Exception ex) {
        LOG.error("Failed to stop Helix rest server, " + ex);
      }
    }
    ServerContext serverContext =
        (ServerContext) getProperty(ContextPropertyKeys.SERVER_CONTEXT.name());
    serverContext.close();
  }

  public void setupSslServer(int port, SslContextFactory sslContextFactory) {
    if (_server != null && port > 0) {
      try {
        HttpConfiguration https = new HttpConfiguration();
        https.addCustomizer(new SecureRequestCustomizer());
        ServerConnector sslConnector = new ServerConnector(
            _server,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(https));
        sslConnector.setPort(port);

        _server.addConnector(sslConnector);

        LOG.info("Helix SSL rest server is ready to start.");
      } catch (Exception ex) {
        LOG.error("Failed to setup Helix SSL rest server, " + ex);
      }
    }
  }
}
