package org.apache.helix.zookeeper.zkclient;

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

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.auth.SASLAuthenticationProvider;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for SASL (Kerberos and DIGEST) authentication detection in ZkClient
 */
public class TestZkClientKerberos extends ZkTestBase {
  
  private ZkClient _zkClient;
  private Configuration _originalJaasConfig;

  @BeforeMethod
  public void setUp() {
    // Save original JAAS configuration
    _originalJaasConfig = Configuration.getConfiguration();
  }

  @AfterMethod
  public void tearDown() {
    // Restore original JAAS configuration
    if (_originalJaasConfig != null) {
      Configuration.setConfiguration(_originalJaasConfig);
    }
    
    if (_zkClient != null && !_zkClient.isClosed()) {
      _zkClient.close();
    }
  }

  /**
   * Test isSaslAuthEnabled returns false when SASL is disabled
   */
  @Test
  public void testIsSaslAuthEnabled_WhenSaslDisabled() throws Exception {
    // Create ZkClient with SASL disabled (default configuration)
    ZkClient.Builder builder = new ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR)
        .setMonitorRootPathOnly(false);
    _zkClient = builder.build();
    _zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    
    // Use reflection to call private isSaslAuthEnabled method
    boolean result = invokeIsSaslAuthEnabled(_zkClient);
    
    // Verify
    Assert.assertFalse(result, "isSaslAuthEnabled should return false when SASL is disabled");
  }

  /**
   * Test isSaslAuthEnabled returns true for a DIGEST login module
   */
  @Test
  public void testIsSaslAuthEnabled_WhenSaslEnabledWithDigestLoginModule() throws Exception {
    buildClientThenEnableSasl("org.apache.zookeeper.server.auth.DigestLoginModule");

    Assert.assertTrue(invokeIsSaslAuthEnabled(_zkClient),
        "isSaslAuthEnabled should return true for a DIGEST login module");
  }

  /**
   * Test isSaslAuthEnabled returns true for a login module Helix does not know by name,
   * because ZooKeeper builds its SASL client for any module in the client JAAS context
   */
  @Test
  public void testIsSaslAuthEnabled_WhenSaslEnabledWithCustomLoginModule() throws Exception {
    buildClientThenEnableSasl("com.example.CustomLoginModule");

    Assert.assertTrue(invokeIsSaslAuthEnabled(_zkClient),
        "isSaslAuthEnabled should return true for a custom login module");
  }

  /**
   * Connect to a server that requires DIGEST SASL, with SASL configured before the client is
   * built, so the connection goes through waitUntilConnected() the way a real deployment does
   */
  @Test
  public void testConnectsToDigestSaslServer() throws Exception {
    File dataDir = Files.createTempDirectory("zk-digest-data").toFile();
    File logDir = Files.createTempDirectory("zk-digest-log").toFile();
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    Configuration.setConfiguration(digestJaasConfiguration());
    // the registry reads this property only once, so make it read it again
    System.setProperty("zookeeper.authProvider.1", SASLAuthenticationProvider.class.getName());
    ProviderRegistry.reset();

    ZooKeeperServer server = new ZooKeeperServer(dataDir, logDir, 2000);
    NIOServerCnxnFactory factory = new NIOServerCnxnFactory();
    try {
      factory.configure(new InetSocketAddress(port), 0);
      factory.startup(server);

      ZkClient.Builder builder = new ZkClient.Builder();
      builder.setZkServer("localhost:" + port)
          .setMonitorRootPathOnly(false);
      _zkClient = builder.build();
      _zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));

      Assert.assertTrue(_zkClient.exists("/"),
          "The client should connect to a DIGEST-enabled server and read the root node");

      // Once SASL completes the state stays at SaslAuthenticated, never going back to SyncConnected,
      // so waiting for the connection after that point must expect SaslAuthenticated
      Assert.assertTrue(_zkClient.waitForKeeperState(KeeperState.SaslAuthenticated, 10, TimeUnit.SECONDS),
          "The client should reach SaslAuthenticated");
      Assert.assertTrue(_zkClient.waitUntilConnected(2, TimeUnit.SECONDS),
          "waitUntilConnected should return once SASL authentication has completed");
    } finally {
      if (_zkClient != null && !_zkClient.isClosed()) {
        _zkClient.close();
      }
      factory.shutdown();
      server.shutdown(true);
      System.clearProperty("zookeeper.authProvider.1");
      ProviderRegistry.reset();
      FileUtils.deleteQuietly(dataDir);
      FileUtils.deleteQuietly(logDir);
    }
  }

  /**
   * JAAS configuration with a DIGEST user on the server side and the same user on the client side
   */
  private static Configuration digestJaasConfiguration() {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        Map<String, String> options = new HashMap<>();
        if ("Server".equals(name)) {
          options.put("user_helix", "secret");
        } else if (ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT.equals(name)) {
          options.put("username", "helix");
          options.put("password", "secret");
        } else {
          return null;
        }
        return new AppConfigurationEntry[] {
            new AppConfigurationEntry(
                "org.apache.zookeeper.server.auth.DigestLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                options
            )
        };
      }
    };
  }

  /**
   * Build the client against the non-SASL test server first, then switch SASL on, so the
   * connection does not wait for a SaslAuthenticated event the test server never sends
   */
  private void buildClientThenEnableSasl(String loginModuleName) {
    ZkClient.Builder builder = new ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR)
        .setMonitorRootPathOnly(false);
    _zkClient = builder.build();
    _zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));

    Configuration.setConfiguration(new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if (ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT.equals(name)) {
          return new AppConfigurationEntry[] {
              new AppConfigurationEntry(
                  loginModuleName,
                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                  new HashMap<>()
              )
          };
        }
        return null;
      }
    });
    ((ZkConnection) _zkClient.getConnection()).getZookeeper().getClientConfig()
        .setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY, "true");
  }


  /**
   * Test isSaslAuthEnabled returns false when JAAS configuration is null
   */
  @Test
  public void testIsSaslAuthEnabled_WhenJaasConfigNull() throws Exception {
    // Setup JAAS configuration that returns null
    Configuration jaasConfig = new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return null;
      }
    };
    Configuration.setConfiguration(jaasConfig);
    
    // Enable SASL via system property
    System.setProperty("zookeeper.sasl.client", "true");
    
    try {
      // Create ZkClient
      ZkClient.Builder builder = new ZkClient.Builder();
      builder.setZkServer(ZkTestBase.ZK_ADDR)
          .setMonitorRootPathOnly(false);
      _zkClient = builder.build();
      _zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
      
      // Use reflection to call private isSaslAuthEnabled method
      boolean result = invokeIsSaslAuthEnabled(_zkClient);
      
      // Verify
      Assert.assertFalse(result, "isSaslAuthEnabled should return false when JAAS config is null");
    } finally {
      System.clearProperty("zookeeper.sasl.client");
    }
  }

  /**
   * Use reflection to invoke private isSaslAuthEnabled method
   */
  private boolean invokeIsSaslAuthEnabled(ZkClient zkClient) throws Exception {
    // The method is in the base class org.apache.helix.zookeeper.zkclient.ZkClient
    Method method = org.apache.helix.zookeeper.zkclient.ZkClient.class.getDeclaredMethod("isSaslAuthEnabled");
    method.setAccessible(true);
    return (Boolean) method.invoke(zkClient);
  }
}
