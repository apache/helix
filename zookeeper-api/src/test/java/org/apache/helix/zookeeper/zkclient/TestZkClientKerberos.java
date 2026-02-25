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

import java.lang.reflect.Method;
import java.util.HashMap;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.client.ZKClientConfig;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for Kerberos authentication detection in ZkClient
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
   * Test isKerberosAuthEnabled returns false when SASL is disabled
   */
  @Test
  public void testIsKerberosAuthEnabled_WhenSaslDisabled() throws Exception {
    // Create ZkClient with SASL disabled (default configuration)
    ZkClient.Builder builder = new ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR)
        .setMonitorRootPathOnly(false);
    _zkClient = builder.build();
    _zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    
    // Use reflection to call private isKerberosAuthEnabled method
    boolean result = invokeIsKerberosAuthEnabled(_zkClient);
    
    // Verify
    Assert.assertFalse(result, "isKerberosAuthEnabled should return false when SASL is disabled");
  }

  /**
   * Test isKerberosAuthEnabled returns false when SASL is enabled but without Kerberos
   */
  @Test
  public void testIsKerberosAuthEnabled_WhenSaslEnabledWithoutKerberos() throws Exception {
    // Setup JAAS configuration with non-Kerberos module
    Configuration jaasConfig = new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if (ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT.equals(name)) {
          return new AppConfigurationEntry[] {
              new AppConfigurationEntry(
                  "com.example.PlainLoginModule",
                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                  new HashMap<>()
              )
          };
        }
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
      
      // Use reflection to call private isKerberosAuthEnabled method
      boolean result = invokeIsKerberosAuthEnabled(_zkClient);
      
      // Verify
      Assert.assertFalse(result, "isKerberosAuthEnabled should return false when Kerberos is not configured");
    } finally {
      System.clearProperty("zookeeper.sasl.client");
    }
  }


  /**
   * Test isKerberosAuthEnabled returns false when JAAS configuration is null
   */
  @Test
  public void testIsKerberosAuthEnabled_WhenJaasConfigNull() throws Exception {
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
      
      // Use reflection to call private isKerberosAuthEnabled method
      boolean result = invokeIsKerberosAuthEnabled(_zkClient);
      
      // Verify
      Assert.assertFalse(result, "isKerberosAuthEnabled should return false when JAAS config is null");
    } finally {
      System.clearProperty("zookeeper.sasl.client");
    }
  }

  /**
   * Use reflection to invoke private isKerberosAuthEnabled method
   */
  private boolean invokeIsKerberosAuthEnabled(ZkClient zkClient) throws Exception {
    // The method is in the base class org.apache.helix.zookeeper.zkclient.ZkClient
    Method method = org.apache.helix.zookeeper.zkclient.ZkClient.class.getDeclaredMethod("isKerberosAuthEnabled");
    method.setAccessible(true);
    return (Boolean) method.invoke(zkClient);
  }
}
