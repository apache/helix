package org.apache.helix;

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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.Message;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.HelixConstants.ChangeType.MESSAGE;


/**
 * This class tests that if there are no incoming events, the onMessage method in message listener
 * will be called by message periodic refresh in callback handler
 */
public class TestPeriodicRefresh extends ZkUnitTestBase {
  private MockManager _manager;
  private final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  private final String instanceName = "instance";
  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";
  protected static final String TEST_DB = "TestDB";
  protected static final int _PARTITIONS = 20;

  public class MockManager extends ZKHelixManager {

    public Map<TestMessageListener, MockCallbackHandler> _testHandlers = new HashMap<>();

    public MockManager(String clusterName, String instanceName, InstanceType instanceType,
        String zkAddress) {
      super(clusterName, instanceName, instanceType, zkAddress);
    }

    public RealmAwareZkClient getClient() {
      return _zkclient;
    }

    void addListener(TestMessageListener listener, PropertyKey propertyKey,
        HelixConstants.ChangeType changeType, Watcher.Event.EventType[] eventType,
        long periodicRefreshInterval) {
      synchronized (this) {
        for (CallbackHandler handler : _handlers) {
          if (handler.getPath().equals(propertyKey.getPath()) && handler.getListener()
              .equals(listener)) {
            return;
          }
        }

        // Use mock call back handler
        CallbackHandler newHandler =
            new MockCallbackHandler(this, _zkclient, propertyKey, listener, eventType, changeType,
                periodicRefreshInterval);
        _testHandlers.put(listener, (MockCallbackHandler) newHandler);
        _handlers.add(newHandler);
      }
    }

    // Use this method to bypass getting messageRefreshInterval from system property to add listener
    public void addMessageListener(TestMessageListener listener, String instanceName,
        String clusterName, long messageRefreshInterval) {
      addListener(listener, new PropertyKey.Builder(clusterName).messages(instanceName),
          HelixConstants.ChangeType.MESSAGE,
          new Watcher.Event.EventType[]{Watcher.Event.EventType.NodeChildrenChanged},
          messageRefreshInterval);
    }
  }

  class MockCallbackHandler extends CallbackHandler {

    public MockCallbackHandler(HelixManager manager, RealmAwareZkClient client,
        PropertyKey propertyKey, Object listener, Watcher.Event.EventType[] eventTypes,
        HelixConstants.ChangeType changeType, long periodicRefreshInterval) {
      super(manager, client, propertyKey, listener, eventTypes, changeType, null,
          periodicRefreshInterval);
    }

    public void invoke(NotificationContext changeContext) {
      if (changeContext.getType() == NotificationContext.Type.INIT
          || changeContext.getType() == NotificationContext.Type.FINALIZE) {
        return;
      }
      if (changeContext.getChangeType() == MESSAGE) {
        MessageListener messageListener = (MessageListener) getListener();
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(getPath());
        messageListener.onMessage(instanceName, null, changeContext);
      }
    }
  }

  public class TestMessageListener implements MessageListener {
    public boolean messageEventReceived = false;

    @Override
    public void onMessage(String instanceName, List<Message> messages,
        NotificationContext changeContext) {
      if (changeContext.getType() == NotificationContext.Type.INIT
          || changeContext.getType() == NotificationContext.Type.FINALIZE) {
        return;
      }
      messageEventReceived = true;
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _gSetupTool.addCluster(clusterName, true);
    _gSetupTool.addResourceToCluster(clusterName, TEST_DB, _PARTITIONS, STATE_MODEL);
    _gSetupTool.addInstanceToCluster(clusterName, instanceName);
    _manager = new MockManager(clusterName, instanceName, InstanceType.PARTICIPANT, ZK_ADDR);
    _manager.connect();
  }

  @AfterSuite
  public void afterSuite() throws IOException {
    _manager.disconnect();
    _gSetupTool.deleteCluster(clusterName);
    super.afterSuite();
  }

  @Test
  public void testWithRefreshAndReset() throws Exception {
    TestMessageListener listener0 = new TestMessageListener();
    // Set interval to 1 so interval + lastEventTime will always < current time (this value has to be > 0)
    _manager.addMessageListener(listener0, instanceName, clusterName, 1);
    boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener0.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    // Test reset method
    CallbackHandler mockHandler = _manager._testHandlers.get(listener0);
    Method resetMethod = CallbackHandler.class.getDeclaredMethod("reset", boolean.class);
    resetMethod.setAccessible(true);

    // Reset(false)
    resetMethod.invoke(mockHandler, false);
    listener0.messageEventReceived = false;
    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener0.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertFalse(result);

    // Reinit
    Method initMethod = CallbackHandler.class.getDeclaredMethod("init");
    initMethod.setAccessible(true);
    initMethod.invoke(mockHandler);
    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener0.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    //Reset(true): shut down
    resetMethod.invoke(mockHandler, true);
    listener0.messageEventReceived = false;
    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener0.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertFalse(result);
  }

  @Test
  public void testWithoutRefresh() throws Exception {
    TestMessageListener listener1 = new TestMessageListener();
    // Not doing refresh
    _manager.addMessageListener(listener1, instanceName, clusterName, -1);
    boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener1.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertFalse(result);
  }

  @Test
  public void testWithPostponedRefresh() throws Exception {
    TestMessageListener listener2 = new TestMessageListener();
    _manager.addMessageListener(listener2, instanceName, clusterName, TestHelper.WAIT_DURATION / 8);
    CallbackHandler mockHandler = _manager._testHandlers.get(listener2);
    Field lastEventTimeField = CallbackHandler.class.getDeclaredField("_lastInvokeTime");
    lastEventTimeField.setAccessible(true);
    // t1
    lastEventTimeField.set(mockHandler, System.currentTimeMillis() + TestHelper.WAIT_DURATION / 8);
    // Make sue when the first refresh is executed (t0), interval (wait_duration/8) + lastEventTime (t1 (very close to t0) + wait_duration/8) > t0
    // So it will sleep and do the refresh after another interval passes which is at t1 + wait_duration/4
    boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener2.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);
  }

  @Test
  public void testWithRefreshAndResetInBatchMode() throws Exception {
    System.setProperty(SystemPropertyKeys.LEGACY_ASYNC_BATCH_MODE_ENABLED, String.valueOf(true));
    TestMessageListener listener3 = new TestMessageListener();
    // Set interval to 1 so interval + lastEventTime will always < current time (this value has to be > 0)
    _manager.addMessageListener(listener3, instanceName, clusterName, 1);
    boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener3.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    // Test reset method
    CallbackHandler mockHandler = _manager._testHandlers.get(listener3);
    Method resetMethod = CallbackHandler.class.getDeclaredMethod("reset", boolean.class);
    resetMethod.setAccessible(true);

    // Reset(false): reset
    resetMethod.invoke(mockHandler, false);
    listener3.messageEventReceived = false;
    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener3.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertFalse(result);

    // Reinit
    Method initMethod = CallbackHandler.class.getDeclaredMethod("init");
    initMethod.setAccessible(true);
    initMethod.invoke(mockHandler);
    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener3.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    //Reset(true): shut down
    resetMethod.invoke(mockHandler, true);
    listener3.messageEventReceived = false;
    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener3.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertFalse(result);

    System.clearProperty(SystemPropertyKeys.LEGACY_ASYNC_BATCH_MODE_ENABLED);
  }

  @Test
  public void testWithPostponedRefreshInBatchMode() throws Exception {
    System.setProperty(SystemPropertyKeys.LEGACY_ASYNC_BATCH_MODE_ENABLED, String.valueOf(true));
    TestMessageListener listener4 = new TestMessageListener();
    _manager.addMessageListener(listener4, instanceName, clusterName, TestHelper.WAIT_DURATION / 8);
    CallbackHandler mockHandler = _manager._testHandlers.get(listener4);
    Field lastEventTimeField = CallbackHandler.class.getDeclaredField("_lastInvokeTime");
    lastEventTimeField.setAccessible(true);
    // t1
    lastEventTimeField.set(mockHandler, System.currentTimeMillis() + TestHelper.WAIT_DURATION / 8);
    // Make sue when the first refresh is executed (t0), interval (wait_duration/8) + lastEventTime (t1 (very close to t0) + wait_duration/8) > t0
    // So it will sleep and do the refresh after another interval passes which is at t1 + wait_duration/4
    boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        return listener4.messageEventReceived;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);
    System.clearProperty(SystemPropertyKeys.LEGACY_ASYNC_BATCH_MODE_ENABLED);
  }
}
