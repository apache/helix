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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.helix.HelixException;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.ServletType;
import org.apache.helix.rest.server.auditlog.AuditLogger;
import org.apache.helix.rest.server.filters.AuditLogFilter;
import org.apache.helix.rest.server.filters.CORSFilter;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class HelixRestServer {
  private static Logger LOG = LoggerFactory.getLogger(HelixRestServer.class);
  // TODO: consider moving the following static context to ServerContext or any other place
  public static SSLContext REST_SERVER_SSL_CONTEXT;

  private Server _server;
  private List<HelixRestNamespace> _helixNamespaces;
  private ServletContextHandler _servletContextHandler;
  private List<AuditLogger> _auditLoggers;

  // Key is name of namespace, value of the resource config of that namespace
  private Map<String, ResourceConfig> _resourceConfigMap;

  public HelixRestServer(String zkAddr, int port, String urlPrefix) {
    this(zkAddr, port, urlPrefix, Collections.emptyList());
  }

  public HelixRestServer(String zkAddr, int port, String urlPrefix, List<AuditLogger> auditLoggers) {
    // Create default namespace using zkAddr
    List<HelixRestNamespace> namespaces =
        ImmutableList.of(new HelixRestNamespace(HelixRestNamespace.DEFAULT_NAMESPACE_NAME,
            HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, zkAddr, true));
    init(namespaces, port, urlPrefix, auditLoggers);
  }

  public HelixRestServer(List<HelixRestNamespace> namespaces, int port, String urlPrefix,
      List<AuditLogger> auditLoggers) {
    init(namespaces, port, urlPrefix, auditLoggers);
  }

  private void init(List<HelixRestNamespace> namespaces, int port, String urlPrefix,
      List<AuditLogger> auditLoggers) {
    if (namespaces.size() == 0) {
      throw new IllegalArgumentException(
          "No namespace specified! Please provide ZOOKEEPER address or namespace manifest.");
    }
    _server = new Server(port);
    _auditLoggers = auditLoggers;
    _resourceConfigMap = new HashMap<>();
    _servletContextHandler = new ServletContextHandler(_server, urlPrefix);
    _helixNamespaces = namespaces;

    // Initialize all namespaces
    try {
      for (HelixRestNamespace namespace : _helixNamespaces) {
        if (namespace.isDefault()) {
          LOG.info("Creating default servlet for default namespace");
          addServlet(namespace, ServletType.DEFAULT_SERVLET);
        } else {
          LOG.info("Creating servlet for namespace " + namespace.getName());
          addServlet(namespace, ServletType.COMMON_SERVLET);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to initialize helix rest server. Tearing down.", e);
      cleanupResourceConfigs();
      throw e;
    }

    // register shut down hook (triggered when the service is shut down)
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  private void addServlet(HelixRestNamespace namespace, ServletType servletType) {
    String resourceConfigMapKey = String.format("%s_%s", servletType.name(), namespace.getName());
    if (_resourceConfigMap.containsKey(resourceConfigMapKey)) {
      throw new IllegalArgumentException(
          String.format("Duplicated namespace name \"%s\"", namespace.getName()));
    }

    // Prepare resource config
    ResourceConfig config = getResourceConfig(namespace, servletType);
    _resourceConfigMap.put(resourceConfigMapKey, config);

    // register servlet listener on path, e.g, http://localhost:8080/admin/v2/namespaces/default/*
    String path = servletType.getServletPath(namespace);
    LOG.info("Initializing Servlet on path: " + path);
    System.out.println("Initializing Servlet on path: " + path);
    _servletContextHandler.addServlet(new ServletHolder(new ServletContainer(config)), path);
  }

  private ResourceConfig getResourceConfig(HelixRestNamespace namespace, ServletType type) {
    ResourceConfig cfg = new ResourceConfig();
    cfg.setApplicationName(namespace.getName());
    cfg.packages(type.getServletPackageArray());

    // Enable the default statistical monitoring MBean for Jersey server
    cfg.property(ServerProperties.MONITORING_STATISTICS_MBEANS_ENABLED, true);
    cfg.property(ContextPropertyKeys.SERVER_CONTEXT.name(),
        new ServerContext(namespace.getMetadataStoreAddress()));
    if (type == ServletType.DEFAULT_SERVLET) {
      cfg.property(ContextPropertyKeys.ALL_NAMESPACES.name(), _helixNamespaces);
    } else {
      cfg.property(ContextPropertyKeys.METADATA.name(), namespace);
    }

    cfg.register(new CORSFilter());
    cfg.register(new AuditLogFilter(_auditLoggers));
    return cfg;
  }

  public void start() throws HelixException, InterruptedException {
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
    cleanupResourceConfigs();
  }

  private void cleanupResourceConfigs() {
    for (Map.Entry<String, ResourceConfig> e : _resourceConfigMap.entrySet()) {
      ServerContext ctx = (ServerContext) e.getValue().getProperty(ContextPropertyKeys.SERVER_CONTEXT.name());
      if (ctx == null) {
        LOG.info("Server context for servlet " + e.getKey() + " is null.");
      } else {
        LOG.info("Closing context for servlet " + e.getKey());
        ctx.close();
      }
    }
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

  /**
   * Register a SSLContext so that it could be used to create HTTPS clients.
   * @param sslContext
   */
  public void registerServerSSLContext(SSLContext sslContext) {
    REST_SERVER_SSL_CONTEXT = sslContext;
  }
}
