package org.apache.helix.zookeeper.impl;

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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.constant.TestConstants;
import org.apache.helix.zookeeper.zkclient.IDefaultNameSpace;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.apache.zookeeper.server.ContainerManager;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;


/**
 * Test base class for various integration tests with an in-memory ZooKeeper.
 */
public class ZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ZkTestBase.class);
  private static final MBeanServerConnection MBEAN_SERVER =
      ManagementFactory.getPlatformMBeanServer();

  // maven surefire-plugin's multiple ZK config keys
  private static final String MULTI_ZK_PROPERTY_KEY = "multiZk";
  private static final String NUM_ZK_PROPERTY_KEY = "numZk";

  public static final String ZK_PREFIX = TestConstants.ZK_PREFIX;
  public static final int ZK_START_PORT = TestConstants.ZK_START_PORT;
  public static final String ZK_ADDR = ZK_PREFIX + ZK_START_PORT;

  /*
   * Multiple ZK references
   */
  // The following maps hold ZK connect string as keys
  protected static final Map<String, ZkServer> _zkServerMap = new ConcurrentHashMap<>();
  protected static final Map<ZkServer, ContainerManager> _zkServerContainerManagerMap = new ConcurrentHashMap<>();
  protected static AtomicLong _fakeElapsed = new AtomicLong(0);
  protected static int _numZk = 1; // Initial value

  /**
   * Advances the fake elapsed time used by the ContainerManager
   * @param additionalTime time to add in milliseconds
   */
  public static void advanceFakeElapsedTime(long additionalTime) {
    _fakeElapsed.addAndGet(additionalTime);
  }

  @BeforeSuite
  public void beforeSuite() throws IOException {
    // Due to ZOOKEEPER-2693 fix, we need to specify whitelist for execute zk commends
    System.setProperty("zookeeper.4lw.commands.whitelist", "*");

    // Set up in-memory ZooKeepers
    setupZooKeepers();

    // Clean up all JMX objects
    for (ObjectName mbean : MBEAN_SERVER.queryNames(null, null)) {
      try {
        MBEAN_SERVER.unregisterMBean(mbean);
      } catch (Exception e) {
        // OK
      }
    }
  }

  @AfterSuite
  public void afterSuite() throws IOException {
    // Clean up all JMX objects
    for (ObjectName mbean : MBEAN_SERVER.queryNames(null, null)) {
      try {
        MBEAN_SERVER.unregisterMBean(mbean);
      } catch (Exception e) {
        // OK
      }
    }

    // Shut down ContainerManagers
    _zkServerContainerManagerMap.values().forEach(ContainerManager::stop);

    // Shut down all ZkServers
    _zkServerMap.values().forEach(ZkServer::shutdown);
  }

  private void setupZooKeepers() {
    // If multi-ZooKeeper is enabled, start more ZKs. Otherwise, just set up one ZK
    String multiZkConfig = System.getProperty(MULTI_ZK_PROPERTY_KEY);
    if (multiZkConfig != null && multiZkConfig.equalsIgnoreCase(Boolean.TRUE.toString())) {
      String numZkFromConfig = System.getProperty(NUM_ZK_PROPERTY_KEY);
      if (numZkFromConfig != null) {
        try {
          _numZk = Math.max(Integer.parseInt(numZkFromConfig), _numZk);
        } catch (Exception e) {
          Assert.fail("Failed to parse the number of ZKs from config!");
        }
      } else {
        Assert.fail("multiZk config is set but numZk config is missing!");
      }
    }

    // Start "numZkFromConfigInt" ZooKeepers
    for (int i = 0; i < _numZk; i++) {
      String zkAddress = ZK_PREFIX + (ZK_START_PORT + i);
      _zkServerMap.computeIfAbsent(zkAddress, ZkTestBase::startZkServer);
      _zkServerContainerManagerMap.computeIfAbsent(_zkServerMap.get(zkAddress), ZkTestBase::createContainerManager);
    }
  }

  /**
   * Creates a ContainerManager with custom elapsed time functionality for a ZkServer
   */
  private static ContainerManager createContainerManager(ZkServer zkServer) {
    try {
      ZooKeeperServer zooKeeperServer = zkServer.getZooKeeperServer();

      Field firstProcessorField = ZooKeeperServer.class.getDeclaredField("firstProcessor");
      firstProcessorField.setAccessible(true);
      RequestProcessor firstProcessor = (RequestProcessor) firstProcessorField.get(zooKeeperServer);

      // Create a ContainerManager with a custom elapsed time logic
      return new ContainerManager(
          zooKeeperServer.getZKDatabase(),
          firstProcessor,
          10, // Check interval in ms
          100,  // Max containers to check per interval
          10 // the max time in milliseconds that a container that has never had any children is retained
      ) {
        @Override
        protected long getElapsed(DataNode node) {
          return _fakeElapsed.get();
        }
      };
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to access firstProcessor field in ZooKeeperServer", e);
    }
  }

  /**
   * Creates an in-memory ZK at the given ZK address.
   * @param zkAddress
   * @return
   */
  protected synchronized static ZkServer startZkServer(final String zkAddress) {
    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";

    // Clean up local directory
    try {
      FileUtils.deleteDirectory(new File(dataDir));
      FileUtils.deleteDirectory(new File(logDir));
    } catch (IOException e) {
      e.printStackTrace();
    }

    IDefaultNameSpace defaultNameSpace = zkClient -> {
    };

    int port = Integer.parseInt(zkAddress.substring(zkAddress.lastIndexOf(':') + 1));
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    System.out.println("Starting ZK server at " + zkAddress);
    zkServer.start();
    return zkServer;
  }
}
