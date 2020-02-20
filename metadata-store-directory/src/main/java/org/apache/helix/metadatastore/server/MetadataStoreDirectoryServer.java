package org.apache.helix.metadatastore.server;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;

import org.apache.helix.metadatastore.server.common.ContextPropertyKeys;
import org.apache.helix.metadatastore.server.common.RestNamespace;
import org.apache.helix.metadatastore.server.common.ServletType;
import org.apache.helix.metadatastore.server.auditlog.AuditLogger;
import org.apache.helix.metadatastore.server.filters.AuditLogFilter;
import org.apache.helix.metadatastore.server.filters.CORSFilter;
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


public class MetadataStoreDirectoryServer {
  private static Logger LOG = LoggerFactory.getLogger(MetadataStoreDirectoryServer.class);
  // TODO: consider moving the following static context to ServerContext or any other place
  public static SSLContext REST_SERVER_SSL_CONTEXT;

  private int _port;
  private String _urlPrefix;
  private Server _server;
  private List<RestNamespace> _restNamespaces;
  private ServletContextHandler _servletContextHandler;
  private List<AuditLogger> _auditLoggers;

  // Key is name of namespace, value of the resource config of that namespace
  private Map<String, ResourceConfig> _resourceConfigMap;

  public MetadataStoreDirectoryServer(String zkAddr, int port, String urlPrefix) {
    this(zkAddr, port, urlPrefix, Collections.<AuditLogger>emptyList());
  }

  public MetadataStoreDirectoryServer(String zkAddr, int port, String urlPrefix,
      List<AuditLogger> auditLoggers) {
    // Create default namespace using zkAddr
    ArrayList<RestNamespace> namespaces = new ArrayList<>();
    namespaces.add(new RestNamespace(RestNamespace.DEFAULT_NAMESPACE_NAME,
        RestNamespace.MetadataStoreType.ZOOKEEPER, zkAddr, true));
    init(namespaces, port, urlPrefix, auditLoggers);
  }

  public MetadataStoreDirectoryServer(List<RestNamespace> namespaces, int port, String urlPrefix,
      List<AuditLogger> auditLoggers) {
    init(namespaces, port, urlPrefix, auditLoggers);
  }

  private void init(List<RestNamespace> namespaces, int port, String urlPrefix,
      List<AuditLogger> auditLoggers) {
    if (namespaces.size() == 0) {
      throw new IllegalArgumentException(
          "No namespace specified! Please provide ZOOKEEPER address or namespace manifest.");
    }
    _port = port;
    _urlPrefix = urlPrefix;
    _server = new Server(_port);
    _auditLoggers = auditLoggers;
    _resourceConfigMap = new HashMap<>();
    _servletContextHandler = new ServletContextHandler(_server, _urlPrefix);
    _restNamespaces = namespaces;

    // Initialize all namespaces.
    // If there is not a default namespace (namespace.isDefault() is false),
    // endpoint "/namespaces" will be disabled.
    try {
      for (RestNamespace namespace : _restNamespaces) {
        if (namespace.isDefault()) {
          LOG.info("Creating default servlet for default namespace");
          prepareServlet(namespace, ServletType.DEFAULT_SERVLET);
        } else {
          LOG.info("Creating common servlet for namespace {}", namespace.getName());
          prepareServlet(namespace, ServletType.COMMON_SERVLET);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to initialize helix rest server. Tearing down.");
      cleanupResourceConfigs();
      throw e;
    }

    // Start special servlet for serving namespaces
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        shutdown();
      }
    }));
  }

  private void prepareServlet(RestNamespace namespace, ServletType type) {
    String resourceConfigMapKey = getResourceConfigMapKey(type, namespace);
    if (_resourceConfigMap.containsKey(resourceConfigMapKey)) {
      throw new IllegalArgumentException(
          String.format("Duplicated namespace name \"%s\"", namespace.getName()));
    }

    // Prepare resource config
    ResourceConfig config = getResourceConfig(namespace, type);
    _resourceConfigMap.put(resourceConfigMapKey, config);

    // Initialize servlet
    initServlet(config, String.format(type.getServletPathSpecTemplate(), namespace.getName()));
  }

  private String getResourceConfigMapKey(ServletType type, RestNamespace namespace) {
    return String.format("%s_%s", type.name(), namespace.getName());
  }

  private ResourceConfig getResourceConfig(RestNamespace namespace, ServletType type) {
    ResourceConfig cfg = new ResourceConfig();
    cfg.packages(type.getServletPackageArray());
    cfg.setApplicationName(namespace.getName());

    // Enable the default statistical monitoring MBean for Jersey server
    cfg.property(ServerProperties.MONITORING_STATISTICS_MBEANS_ENABLED, true);
    if (type == ServletType.DEFAULT_SERVLET) {
      cfg.property(ContextPropertyKeys.ALL_NAMESPACES.name(), _restNamespaces);
    } else {
      cfg.property(ContextPropertyKeys.METADATA.name(), namespace);
    }

    cfg.register(new CORSFilter());
    cfg.register(new AuditLogFilter(_auditLoggers));
    return cfg;
  }

  private void initServlet(ResourceConfig cfg, String servletPathSpec) {
    ServletHolder servlet = new ServletHolder(new ServletContainer(cfg));
    _servletContextHandler.addServlet(servlet, servletPathSpec);
  }

  public void start() throws Exception {
    try {
      _server.start();
    } catch (Exception ex) {
      LOG.error("Failed to start Metadata Store Directory REST server, " + ex);
      throw new Exception("Failed to start Metadata Store Directory REST server! " + ex);
    }

    LOG.info("Metadata Store Directory REST server started!");
  }

  public void join() {
    if (_server != null) {
      try {
        _server.join();
      } catch (InterruptedException e) {
        LOG.warn("Join on Metadata Store Directory REST server get interrupted!" + e);
      }
    }
  }

  public void shutdown() {
    if (_server != null) {
      try {
        _server.stop();
        LOG.info("Metadata Store Directory REST server stopped!");
      } catch (Exception ex) {
        LOG.error("Failed to stop Metadata Store Directory REST server, " + ex);
      }
    }
    cleanupResourceConfigs();
  }

  private void cleanupResourceConfigs() {
    // TODO: Add any cleanup logic here
  }

  public void setupSslServer(int port, SslContextFactory sslContextFactory) {
    if (_server != null && port > 0) {
      try {
        HttpConfiguration https = new HttpConfiguration();
        https.addCustomizer(new SecureRequestCustomizer());
        ServerConnector sslConnector = new ServerConnector(_server,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(https));
        sslConnector.setPort(port);

        _server.addConnector(sslConnector);

        LOG.info("Metadata Store Directory REST SSL server is ready to start.");
      } catch (Exception ex) {
        LOG.error("Failed to setup Metadata Store Directory REST SSL server, " + ex);
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
