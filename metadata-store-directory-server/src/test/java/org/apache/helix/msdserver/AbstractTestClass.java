package org.apache.helix.msdserver;

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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.msdserver.server.common.RestNamespace;
import org.apache.helix.msdserver.server.MetadataStoreDirectoryServer;
import org.apache.helix.msdserver.server.auditlog.AuditLog;
import org.apache.helix.msdserver.server.auditlog.AuditLogger;
import org.apache.helix.msdserver.server.filters.AuditLogFilter;
import org.apache.helix.msdserver.server.resources.AbstractResource;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.spi.TestContainer;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;


public class AbstractTestClass extends JerseyTestNg.ContainerPerClassTest {
  /**
   * Constants for multi-ZK environment.
   */
  private static final String MULTI_ZK_PROPERTY_KEY = "multiZk";
  private static final String NUM_ZK_PROPERTY_KEY = "numZk";
  private static final String ZK_PREFIX = "localhost:";
  private static final int ZK_START_PORT = 2123;
  // The following map must be a static map because it needs to be shared throughout tests
  protected static final Map<String, ZkServer> ZK_SERVER_MAP = new HashMap<>();

  // For a single-ZK environment
  protected static final String ZK_ADDR = "localhost:2123";
  private static ZkServer _zkServer;
  protected static HelixZkClient _gZkClient;
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static boolean _init = false;
  private static MockAuditLogger _auditLogger = new MockAuditLogger();
  private static MetadataStoreDirectoryServer _MetadataStoreDirectoryServer;

  // TODO: remove?
  protected static class MockAuditLogger implements AuditLogger {
    List<AuditLog> _auditLogList = new ArrayList<>();

    @Override
    public void write(AuditLog auditLog) {
      _auditLogList.add(auditLog);
    }

    public void clearupLogs() {
      _auditLogList.clear();
    }

    public List<AuditLog> getAuditLogs() {
      return _auditLogList;
    }
  }

  @Override
  protected Application configure() {
    // Configure server context
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.packages(AbstractResource.class.getPackage().getName());
    resourceConfig.register(new AuditLogFilter(Collections.singletonList(new MockAuditLogger())));

    return resourceConfig;
  }

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return (baseUri, deploymentContext) -> new TestContainer() {

      @Override
      public ClientConfig getClientConfig() {
        return null;
      }

      @Override
      public URI getBaseUri() {
        return baseUri;
      }

      @Override
      public void start() {
        if (_MetadataStoreDirectoryServer == null) {
          // Create namespace manifest map
          List<RestNamespace> namespaces = new ArrayList<>();

          // Add default namesapce
          namespaces.add(new RestNamespace(ZK_ADDR));
          try {
            _MetadataStoreDirectoryServer =
                new MetadataStoreDirectoryServer(namespaces, baseUri.getPort(), baseUri.getPath(),
                    Collections.singletonList(_auditLogger));
            _MetadataStoreDirectoryServer.start();
          } catch (Exception ex) {
            throw new TestContainerException(ex);
          }
        }
      }

      @Override
      public void stop() {
      }
    };
  }

  @BeforeSuite
  public void beforeSuite() throws Exception {
    if (!_init) {
      setupZooKeepers();

      // TODO: use logging.properties file to config java.util.logging.Logger levels
      java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
      topJavaLogger.setLevel(Level.WARNING);

      HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();

      clientConfig.setZkSerializer(new ZNRecordSerializer());
      _gZkClient = DedicatedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR), clientConfig);

      // wait for the web service to start
      Thread.sleep(100);
      _init = true;
    }
  }

  @AfterSuite
  public void afterSuite() {
    if (_gZkClient != null) {
      _gZkClient.close();
      _gZkClient = null;
    }

    if (_MetadataStoreDirectoryServer != null) {
      _MetadataStoreDirectoryServer.shutdown();
      _MetadataStoreDirectoryServer = null;
    }

    // Stop all ZkServers
    ZK_SERVER_MAP.forEach((zkAddr, zkServer) -> TestHelper.stopZkServer(zkServer));
  }

  private void setupZooKeepers() {
    // start zk
    try {
      if (_zkServer == null) {
        _zkServer = TestHelper.startZkServer(ZK_ADDR);
        Assert.assertNotNull(_zkServer);
        ZK_SERVER_MAP.put(ZK_ADDR, _zkServer);
      }
    } catch (Exception e) {
      Assert.fail(String.format("Failed to start ZK servers: %s", e.toString()));
    }

    // Start additional ZKs in a multi-ZK setup if applicable
    String multiZkConfig = System.getProperty(MULTI_ZK_PROPERTY_KEY);
    if (multiZkConfig != null && multiZkConfig.equalsIgnoreCase(Boolean.TRUE.toString())) {
      String numZkFromConfig = System.getProperty(NUM_ZK_PROPERTY_KEY);
      if (numZkFromConfig != null) {
        try {
          int numZkFromConfigInt = Integer.parseInt(numZkFromConfig);
          // Start (numZkFromConfigInt - 2) ZooKeepers
          for (int i = 2; i < numZkFromConfigInt; i++) {
            String zkAddr = ZK_PREFIX + (ZK_START_PORT + i);
            ZkServer zkServer = TestHelper.startZkServer(zkAddr);
            Assert.assertNotNull(zkServer);
            ZK_SERVER_MAP.put(zkAddr, zkServer);
          }
        } catch (Exception e) {
          Assert.fail("Failed to create multiple ZooKeepers!");
        }
      }
    }
  }

  protected static ZNRecord toZNRecord(String data) throws IOException {
    return OBJECT_MAPPER.reader(ZNRecord.class).readValue(data);
  }

  protected String get(String uri, Map<String, String> queryParams, int expectedReturnStatus,
      boolean expectBodyReturned) {
    WebTarget webTarget = target(uri);
    if (queryParams != null) {
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
    }
    final Response response = webTarget.request().get();
    Assert.assertEquals(response.getStatus(), expectedReturnStatus);

    // NOT_FOUND and BAD_REQUEST will throw text based html
    if (expectedReturnStatus != Response.Status.NOT_FOUND.getStatusCode()
        && expectedReturnStatus != Response.Status.BAD_REQUEST.getStatusCode()) {
      Assert.assertEquals(response.getMediaType().getType(), "application");
    } else {
      Assert.assertEquals(response.getMediaType().getType(), "text");
    }

    String body = response.readEntity(String.class);
    if (expectBodyReturned) {
      Assert.assertNotNull(body);
    }

    return body;
  }

  protected void put(String uri, Map<String, String> queryParams, Entity entity,
      int expectedReturnStatus) {
    WebTarget webTarget = target(uri);
    if (queryParams != null) {
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
    }
    Response response = webTarget.request().put(entity);
    Assert.assertEquals(response.getStatus(), expectedReturnStatus);
  }

  protected void post(String uri, Map<String, String> queryParams, Entity entity,
      int expectedReturnStatus) {
    WebTarget webTarget = target(uri);
    if (queryParams != null) {
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
    }
    Response response = webTarget.request().post(entity);
    Assert.assertEquals(response.getStatus(), expectedReturnStatus);
  }

  protected void delete(String uri, int expectedReturnStatus) {
    final Response response = target(uri).request().delete();
    Assert.assertEquals(response.getStatus(), expectedReturnStatus);
  }
}
