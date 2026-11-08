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
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.impl.TestHelper;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Integration tests for SASL (Kerberos, DIGEST and other mechanisms) authentication detection in
 * ZkClient
 */
public class TestZkClientSasl extends ZkTestBase {

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
   * Test isSaslAuthEnabled returns false when the SASL client is disabled, even though the JAAS
   * configuration has an entry for the client context
   */
  @Test
  public void testIsSaslAuthEnabled_WhenSaslDisabled() throws Exception {
    Configuration.setConfiguration(jaasConfiguration("org.apache.zookeeper.server.auth.DigestLoginModule"));
    System.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY, "false");

    try {
      ZkClient.Builder builder = new ZkClient.Builder();
      builder.setZkServer(ZkTestBase.ZK_ADDR)
          .setMonitorRootPathOnly(false);
      _zkClient = builder.build();
      _zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));

      Assert.assertFalse(invokeIsSaslAuthEnabled(_zkClient),
          "isSaslAuthEnabled should return false when the SASL client is disabled");
    } finally {
      System.clearProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY);
    }
  }

  @DataProvider(name = "saslLoginModules")
  public Object[][] saslLoginModules() {
    return new Object[][] {
        {"org.apache.zookeeper.server.auth.DigestLoginModule"},
        // a login module Helix does not know by name
        {"com.example.CustomLoginModule"}
    };
  }

  /**
   * Test isSaslAuthEnabled returns true for any login module, because ZooKeeper builds its SASL
   * client for any module in the client JAAS context
   */
  @Test(dataProvider = "saslLoginModules")
  public void testIsSaslAuthEnabled_WhenSaslEnabled(String loginModuleName) throws Exception {
    buildClientThenEnableSasl(loginModuleName);

    Assert.assertTrue(invokeIsSaslAuthEnabled(_zkClient),
        "isSaslAuthEnabled should return true for the login module " + loginModuleName);
  }

  /**
   * Connect to a server that requires DIGEST SASL, with SASL configured before the client is
   * built, so the connection goes through waitUntilConnected() the way a real deployment does
   */
  @Test
  public void testConnectsToDigestSaslServer() throws Exception {
    File dataDir = Files.createTempDirectory("zk-digest-data").toFile();
    File logDir = Files.createTempDirectory("zk-digest-log").toFile();
    ZooKeeperServer server = null;
    NIOServerCnxnFactory factory = null;
    try {
      Configuration.setConfiguration(digestJaasConfiguration());
      // the registry reads this property only once, so make it read it again
      System.setProperty("zookeeper.authProvider.1", SASLAuthenticationProvider.class.getName());
      ProviderRegistry.reset();
      // the server closes a session that does not authenticate, like in a secured deployment
      System.setProperty("zookeeper.sessionRequireClientSASLAuth", "true");

      int port = TestHelper.getRandomPort();
      server = new ZooKeeperServer(dataDir, logDir, 2000);
      factory = new NIOServerCnxnFactory();
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
      if (factory != null) {
        factory.shutdown();
      }
      if (server != null) {
        server.shutdown(true);
      }
      System.clearProperty("zookeeper.authProvider.1");
      System.clearProperty("zookeeper.sessionRequireClientSASLAuth");
      ProviderRegistry.reset();
      FileUtils.deleteQuietly(dataDir);
      FileUtils.deleteQuietly(logDir);
    }
  }

  /**
   * When ZooKeeper cannot set up its SASL client, here because the login fails, it connects
   * without SASL to a server that allows it, and the session is usable. The connection state then
   * stays at SyncConnected and never reaches SaslAuthenticated, so waitUntilConnected() must not
   * wait for SaslAuthenticated.
   */
  @Test
  public void testConnectsWhenSaslLoginFails() throws Exception {
    FailingLoginModule.LOGIN_ATTEMPTS.set(0);
    Configuration.setConfiguration(jaasConfiguration(FailingLoginModule.class.getName()));

    ZkClient.Builder builder = new ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR)
        .setConnectionTimeout(10 * 1000)
        .setMonitorRootPathOnly(false);
    _zkClient = builder.build();
    _zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));

    Assert.assertTrue(FailingLoginModule.LOGIN_ATTEMPTS.get() > 0,
        "The SASL login should have been attempted");
    Assert.assertTrue(invokeIsSaslAuthEnabled(_zkClient),
        "SASL is configured, only its login fails");
    Assert.assertTrue(_zkClient.exists("/"),
        "The session should be usable although the SASL login failed");
    Assert.assertTrue(_zkClient.waitUntilConnected(2, TimeUnit.SECONDS),
        "waitUntilConnected should return when ZooKeeper connected without SASL");
  }

  /**
   * A login module that always fails, like a missing keytab or an expired ticket cache
   */
  public static class FailingLoginModule implements LoginModule {
    static final AtomicInteger LOGIN_ATTEMPTS = new AtomicInteger();

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
        Map<String, ?> sharedState, Map<String, ?> options) {
    }

    @Override
    public boolean login() throws LoginException {
      LOGIN_ATTEMPTS.incrementAndGet();
      throw new LoginException("The login fails on purpose");
    }

    @Override
    public boolean commit() {
      return false;
    }

    @Override
    public boolean abort() {
      return false;
    }

    @Override
    public boolean logout() {
      return false;
    }
  }

  /**
   * JAAS configuration with the given login module for the client context
   */
  private static Configuration jaasConfiguration(String loginModuleName) {
    return new Configuration() {
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
    };
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

    Configuration.setConfiguration(jaasConfiguration(loginModuleName));
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
