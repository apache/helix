package org.apache.helix.manager.zk;/*
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

import java.lang.reflect.Field;

import org.apache.helix.InstanceType;
import org.apache.helix.common.ZkTestBase;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;

import static org.mockito.Mockito.*;
import org.mockito.ArgumentCaptor;

/**
 * Test class to verify that ZKHelixManager uses _lastQueuedSessionID for logging
 * in handleStateChanged() instead of the potentially stale _sessionId.
 */
public class TestZKHelixManagerSessionLogging extends ZkTestBase {
  private static final String CLUSTER_NAME = "TestCluster";
  private static final String INSTANCE_NAME = "TestInstance";

  private ZKHelixManager manager;

  @BeforeMethod
  public void setUp() {
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, INSTANCE_NAME);

    manager = new ZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR);
  }

  @AfterMethod
  public void tearDown() {
    if (manager != null && manager.isConnected()) {
      manager.disconnect();
    }

    _gSetupTool.deleteCluster(CLUSTER_NAME);
  }

  @DataProvider(name = "keeperStates")
  public Object[][] keeperStatesProvider() {
    return new Object[][] {
        {KeeperState.Expired, "warn"},
        {KeeperState.Disconnected, "warn"},
        {KeeperState.Closed, "info"}
    };
  }

  @Test(dataProvider = "keeperStates")
  public void testHandleStateChangedUsesLastQueuedSessionIdForLogging(KeeperState state, String logLevel) throws Exception {
    String lastQueuedSessionId = "1234567";
    String staleSessionId = "7654321";

    Field lastQueuedSessionIdField = ZKHelixManager.class.getDeclaredField("_lastQueuedSessionID");
    Field sessionIdField = ZKHelixManager.class.getDeclaredField("_sessionId");
    lastQueuedSessionIdField.setAccessible(true);
    sessionIdField.setAccessible(true);

    lastQueuedSessionIdField.set(manager, lastQueuedSessionId);
    sessionIdField.set(manager, staleSessionId);

    // Mock the logger to capture log calls
    Logger mockLogger = mock(Logger.class);

    Field loggerField = ZKHelixManager.class.getDeclaredField("LOG");
    loggerField.setAccessible(true);

    // Remove final modifier to allow replacement
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);

    Logger originalLogger = (Logger) loggerField.get(null);
    loggerField.set(null, mockLogger);

    try {
      // simulate the state change event
      manager.handleStateChanged(state);

      // Verify the appropriate log level was called and capture the log message
      ArgumentCaptor<String> logMessageCaptor = ArgumentCaptor.forClass(String.class);
      if ("warn".equals(logLevel)) {
        verify(mockLogger).warn(logMessageCaptor.capture());
      } else if ("info".equals(logLevel)) {
        verify(mockLogger).info(logMessageCaptor.capture());
      }

      String actualLogMessage = logMessageCaptor.getValue();

      // log message should contain _lastQueuedSessionID value
      Assert.assertTrue(actualLogMessage.contains(lastQueuedSessionId),
          "Log message for " + state + " state should contain _lastQueuedSessionID value (" +
              lastQueuedSessionId + ") " +
              "but actual log was: " + actualLogMessage);

      // log message should not contain _sessionId value
      Assert.assertFalse(actualLogMessage.contains(staleSessionId),
          "Log message for " + state + " state should NOT contain _sessionId value (" + staleSessionId + ") " +
              "but actual log was: " + actualLogMessage);

    } finally {
      loggerField.set(null, originalLogger);
    }
  }
}