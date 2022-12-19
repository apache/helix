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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import com.codahale.metrics.jmx.JmxReporter;
import org.apache.helix.HelixException;
import org.apache.helix.rest.acl.AclRegister;
import org.apache.helix.rest.acl.NoopAclRegister;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.ServletType;
import org.apache.helix.rest.server.auditlog.AuditLogger;
import org.apache.helix.rest.server.authValidator.AuthValidator;
import org.apache.helix.rest.server.authValidator.NoopAuthValidator;
import org.apache.helix.rest.server.filters.AuditLogFilter;
import org.apache.helix.rest.server.filters.CORSFilter;
import org.apache.helix.rest.server.filters.ClusterAuthFilter;
import org.apache.helix.rest.server.filters.NamespaceAuthFilter;
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
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HelixRestServer {
  private static Logger LOG = LoggerFactory.getLogger(HelixRestServer.class);

  private static final String REST_DOMAIN = "org.apache.helix.rest";
  private static final String CORS_ENABLED = "cors.enabled";

  // TODO: consider moving the following static context to ServerContext or any other place
  public static SSLContext REST_SERVER_SSL_CONTEXT;

  private int _port;
  private String _urlPrefix;
  private Server _server;
  private List<JmxReporter> _jmxReporterList;
  private List<HelixRestNamespace> _helixNamespaces;
  private ServletContextHandler _servletContextHandler;
  private List<AuditLogger> _auditLoggers;
  private AuthValidator _clusterAuthValidator;
  private AuthValidator _namespaceAuthValidator;
  private AclRegister _aclRegister;

  // Key is name of namespace, value of the resource config of that namespace
  private Map<String, ResourceConfig> _resourceConfigMap;

  public HelixRestServer(String zkAddr, int port, String urlPrefix) {
    this(zkAddr, port, urlPrefix, Collections.<AuditLogger>emptyList());
  }

  public HelixRestServer(String zkAddr, int port, String urlPrefix, List<AuditLogger> auditLoggers) {
    // Create default namespace using zkAddr
    ArrayList<HelixRestNamespace> namespaces = new ArrayList<>();
    namespaces.add(new HelixRestNamespace(HelixRestNamespace.DEFAULT_NAMESPACE_NAME,
        HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, zkAddr, true));
    init(namespaces, port, urlPrefix, auditLoggers);
  }

  public HelixRestServer(List<HelixRestNamespace> namespaces, int port, String urlPrefix,
      List<AuditLogger> auditLoggers) {
    init(namespaces, port, urlPrefix, auditLoggers);
  }

  public HelixRestServer(List<HelixRestNamespace> namespaces, int port, String urlPrefix,
      List<AuditLogger> auditLoggers, AuthValidator clusterAuthValidator,
      AuthValidator namespaceAuthValidator, AclRegister aclRegister) {
    init(namespaces, port, urlPrefix, auditLoggers, clusterAuthValidator, namespaceAuthValidator,
        aclRegister);
  }

  private void init(List<HelixRestNamespace> namespaces, int port, String urlPrefix,
      List<AuditLogger> auditLoggers) {
    init(namespaces, port, urlPrefix, auditLoggers, new NoopAuthValidator(),
        new NoopAuthValidator(), new NoopAclRegister());
  }

  private void init(List<HelixRestNamespace> namespaces, int port, String urlPrefix,
      List<AuditLogger> auditLoggers, AuthValidator clusterAuthValidator,
      AuthValidator namespaceAuthValidator, AclRegister aclRegister) {
    if (namespaces.size() == 0) {
      throw new IllegalArgumentException(
          "No namespace specified! Please provide ZOOKEEPER address or namespace manifest.");
    }
    _port = port;
    _urlPrefix = urlPrefix;
    _server = new Server(_port);
    _jmxReporterList = new ArrayList<>();
    _auditLoggers = auditLoggers;
    _resourceConfigMap = new HashMap<>();
    _servletContextHandler = new ServletContextHandler(_server, _urlPrefix);
    _helixNamespaces = namespaces;
    _clusterAuthValidator = clusterAuthValidator;
    _namespaceAuthValidator = namespaceAuthValidator;
    _aclRegister = aclRegister;

    // Initialize all namespaces.
    // If there is not a default namespace (namespace.isDefault() is false),
    // endpoint "/namespaces" will be disabled.
    try {
      for (HelixRestNamespace namespace : _helixNamespaces) {
        if (namespace.isDefault()) {
          LOG.info("Creating default servlet for default namespace");
          prepareServlet(namespace, ServletType.DEFAULT_SERVLET);
        } else {
          LOG.info("Creating common servlet for namespace {}", namespace.getName());
          prepareServlet(namespace, ServletType.COMMON_SERVLET);
        }
      }
    } catch (Exception e) {
      cleanupResourceConfigs();
      throw e;
    }

    // Start special servlet for serving namespaces
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override public void run() {
        shutdown();
      }
    }));
  }

  private void prepareServlet(HelixRestNamespace namespace, ServletType type) {
    String resourceConfigMapKey = getResourceConfigMapKey(type, namespace);
    if (_resourceConfigMap.containsKey(resourceConfigMapKey)) {
      throw new IllegalArgumentException(
          String.format("Duplicated namespace name \"%s\"", namespace.getName()));
    }

    // Prepare resource config
    ResourceConfig config = getResourceConfig(namespace, type);
    _resourceConfigMap.put(resourceConfigMapKey, config);

    initMetricRegistry(config, namespace.getName());

    // Initialize servlet
    initServlet(config, String.format(type.getServletPathSpecTemplate(), namespace.getName()));
  }

  private String getResourceConfigMapKey(ServletType type, HelixRestNamespace namespace) {
    return String.format("%s_%s", type.name(), namespace.getName());
  }

  protected ResourceConfig getResourceConfig(HelixRestNamespace namespace, ServletType type) {
    ResourceConfig cfg = new ResourceConfig();
    cfg.packages(type.getServletPackageArray());
    cfg.setApplicationName(namespace.getName());

    cfg.property(ContextPropertyKeys.SERVER_CONTEXT.name(),
        new ServerContext(namespace.getMetadataStoreAddress(), namespace.isMultiZkEnabled(),
            namespace.getMsdsEndpoint()));
    if (type == ServletType.DEFAULT_SERVLET) {
      cfg.property(ContextPropertyKeys.ALL_NAMESPACES.name(), _helixNamespaces);
    }
    cfg.property(ContextPropertyKeys.METADATA.name(), namespace);
    cfg.property(ContextPropertyKeys.ACL_REGISTER.name(), _aclRegister);

    if (Boolean.getBoolean(CORS_ENABLED)) {
      // NOTE: CORS is disabled by default unless otherwise specified in System Properties.
      cfg.register(new CORSFilter());
    }
    cfg.register(new AuditLogFilter(_auditLoggers));
    cfg.register(new ClusterAuthFilter(_clusterAuthValidator));
    cfg.register(new NamespaceAuthFilter(_namespaceAuthValidator));
    return cfg;
  }

  /*
   * Initialize metric registry and jmx reporter for each namespace.
   */
  private void initMetricRegistry(ResourceConfig cfg, String namespace) {
    MetricRegistry metricRegistry = new MetricRegistry();
    // Set the sliding time window to be 1 minute for now
    Supplier<Reservoir> reservoirSupplier = () -> {
      return new SlidingTimeWindowReservoir(60, TimeUnit.SECONDS);
    };
    cfg.register(
        new InstrumentedResourceMethodApplicationListener(metricRegistry, Clock.defaultClock(),
            false, reservoirSupplier));
    SharedMetricRegistries.add(namespace, metricRegistry);

    // JmxReporter doesn't have an option to specify namespace for each servlet,
    // we use a customized object name factory to get and insert namespace to object name.
    JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
        .inDomain(REST_DOMAIN)
        .createsObjectNamesWith(new HelixRestObjectNameFactory(namespace))
        .build();
    jmxReporter.start();
    _jmxReporterList.add(jmxReporter);
  }

  private void initServlet(ResourceConfig cfg, String servletPathSpec) {
    ServletHolder servlet = new ServletHolder(new ServletContainer(cfg));
    _servletContextHandler.addServlet(servlet, servletPathSpec);
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

  public synchronized void shutdown() {
    if (_server != null) {
      try {
        _server.stop();
        LOG.info("Helix rest server stopped!");
      } catch (Exception ex) {
        LOG.error("Failed to stop Helix rest server, " + ex);
      }
    }
    _jmxReporterList.forEach(JmxReporter::stop);
    _jmxReporterList.clear();
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
