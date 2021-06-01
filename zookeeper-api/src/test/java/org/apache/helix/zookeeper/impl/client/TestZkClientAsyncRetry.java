package org.apache.helix.zookeeper.impl.client;

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

import java.lang.management.ManagementFactory;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallMonitorContext;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncRetryCallContext;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.apache.helix.zookeeper.zkclient.metric.ZkClientMonitor;
import org.apache.helix.zookeeper.zkclient.metric.ZkClientPathMonitor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS;

/**
 * Note this is a whitebox test to test the async operation callback/context.
 * We don't have a good way to simulate an async ZK operation failure in the server side yet.
 */
public class TestZkClientAsyncRetry extends ZkTestBase {
  private final String TEST_ROOT = String.format("/%s", getClass().getSimpleName());
  private final String NODE_PATH = TEST_ROOT + "/async";
  // Retry wait time is set to 3 times of the retry interval so as to leave extra buffer in case
  // the test environment is slow. Extra wait time won't impact the test logic.
  private final long RETRY_OPS_WAIT_TIMEOUT_MS = 3 * MockAsyncZkClient.RETRY_INTERVAL_MS;

  final String TEST_TAG = "test_tag";
  final String TEST_KEY = "test_key";
  final String TEST_INSTANCE = "test_instance";

  private org.apache.helix.zookeeper.zkclient.ZkClient _zkClient;
  private String _zkServerAddress;

  private final MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();
  private ZkClientMonitor _monitor;
  ObjectName _rootName;
  int _readFailures;
  int _writeFailures;

  @BeforeClass
  public void beforeClass() throws JMException {
    _zkClient = _zkServerMap.values().iterator().next().getZkClient();
    _zkServerAddress = _zkClient.getServers();
    _zkClient.createPersistent(TEST_ROOT);

    _monitor = new ZkClientMonitor(TEST_TAG, TEST_KEY, TEST_INSTANCE, false, null);
    _monitor.register();

    _rootName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY, TEST_INSTANCE,
        ZkClientPathMonitor.PredefinedPath.Root.name());
    _readFailures = 0;
    _writeFailures = 0;
  }

  @AfterClass
  public void afterClass() {
    _monitor.unregister();
    _zkClient.deleteRecursively(TEST_ROOT);
    _zkClient.close();
  }

  private boolean needRetry(int rc) {
    switch (KeeperException.Code.get(rc)) {
      /** Connection to the server has been lost */
      case CONNECTIONLOSS:
        /** The session has been expired by the server */
      case SESSIONEXPIRED:
        /** Session moved to another server, so operation is ignored */
      case SESSIONMOVED:
        return true;
      default:
        return false;
    }
  }

  private boolean waitAsyncOperation(ZkAsyncCallbacks.DefaultCallback callback, long timeout) {
    final boolean[] ret = { false };
    Thread waitThread = new Thread(() -> ret[0] = callback.waitForSuccess());
    waitThread.start();
    try {
      waitThread.join(timeout);
      waitThread.interrupt();
      return ret[0];
    } catch (InterruptedException e) {
      return false;
    }
  }

  private ObjectName buildObjectName(String tag, String key, String instance)
      throws MalformedObjectNameException {
    return ZkClientMonitor.getObjectName(tag, key, instance);
  }

  private ObjectName buildPathMonitorObjectName(String tag, String key, String instance,
      String path) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s,%s=%s", buildObjectName(tag, key, instance).toString(),
        ZkClientPathMonitor.MONITOR_PATH, path));
  }

  @Test
  public void testAsyncRetryCategories() throws JMException {
    MockAsyncZkClient testZkClient = new MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      // Loop all possible error codes to test async create.
      // Only connectivity issues will be retried, the other issues will be return error immediately.
      for (KeeperException.Code code : KeeperException.Code.values()) {
        if (code == KeeperException.Code.OK) {
          continue;
        }
        ZkAsyncCallbacks.CreateCallbackHandler createCallback =
            new ZkAsyncCallbacks.CreateCallbackHandler();
        Assert.assertEquals(createCallback.getRc(), KeeperException.Code.APIERROR.intValue());
        testZkClient.setAsyncCallRC(code.intValue());
        if (code == CONNECTIONLOSS || code == KeeperException.Code.SESSIONEXPIRED
            || code == KeeperException.Code.SESSIONMOVED) {
          // Async create will be pending due to the mock error rc is retryable.
          testZkClient.asyncCreate(NODE_PATH, null, CreateMode.PERSISTENT, createCallback);
          Assert.assertFalse(createCallback.isOperationDone());
          Assert.assertEquals(createCallback.getRc(), code.intValue());
          // Change the mock response
          testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
          // Async retry will succeed now. Wait until the operation is successfully done and verify.
          Assert.assertTrue(waitAsyncOperation(createCallback, RETRY_OPS_WAIT_TIMEOUT_MS));
          Assert.assertEquals(createCallback.getRc(), KeeperException.Code.OK.intValue());
          Assert.assertTrue(testZkClient.exists(NODE_PATH));
          Assert.assertTrue(testZkClient.getAndResetRetryCount() >= 1);
        } else {
          // Async create will fail due to the mock error rc is not recoverable.
          testZkClient.asyncCreate(NODE_PATH, null, CreateMode.PERSISTENT, createCallback);
          Assert.assertTrue(waitAsyncOperation(createCallback, RETRY_OPS_WAIT_TIMEOUT_MS));
          Assert.assertEquals(createCallback.getRc(), code.intValue());
          Assert.assertEquals(testZkClient.getAndResetRetryCount(), 0);
          ++_writeFailures;
        }
        Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
            ZkClientPathMonitor.PredefinedMetricDomains.WriteAsyncFailureCounter.toString()),
            _writeFailures);
        testZkClient.delete(NODE_PATH);
        Assert.assertFalse(testZkClient.exists(NODE_PATH));
      }
    } finally {
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  @Test(dependsOnMethods = "testAsyncRetryCategories")
  public void testAsyncWriteRetry() throws JMException {
    MockAsyncZkClient testZkClient = new MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      // 1. Test async set retry
      ZkAsyncCallbacks.SetDataCallbackHandler setCallback =
          new ZkAsyncCallbacks.SetDataCallbackHandler();
      Assert.assertEquals(setCallback.getRc(), KeeperException.Code.APIERROR.intValue());

      tmpRecord.setSimpleField("test", "data");
      testZkClient.setAsyncCallRC(CONNECTIONLOSS.intValue());
      // Async set will be pending due to the mock error rc is retryable.
      testZkClient.asyncSetData(NODE_PATH, tmpRecord, -1, setCallback);
      Assert.assertFalse(setCallback.isOperationDone());
      Assert.assertEquals(setCallback.getRc(), CONNECTIONLOSS.intValue());
      // Change the mock return code.
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      // Async retry will succeed now. Wait until the operation is successfully done and verify.
      Assert.assertTrue(waitAsyncOperation(setCallback, RETRY_OPS_WAIT_TIMEOUT_MS));
      Assert.assertEquals(setCallback.getRc(), KeeperException.Code.OK.intValue());
      Assert.assertEquals(((ZNRecord) testZkClient.readData(NODE_PATH)).getSimpleField("test"),
          "data");
      Assert.assertTrue(testZkClient.getAndResetRetryCount() >= 1);
      // Check failure metric, which should be unchanged because the operation succeeded
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.WriteAsyncFailureCounter.toString()),
          _writeFailures);

      // 2. Test async delete
      ZkAsyncCallbacks.DeleteCallbackHandler deleteCallback =
          new ZkAsyncCallbacks.DeleteCallbackHandler();
      Assert.assertEquals(deleteCallback.getRc(), KeeperException.Code.APIERROR.intValue());

      testZkClient.setAsyncCallRC(CONNECTIONLOSS.intValue());
      // Async delete will be pending due to the mock error rc is retryable.
      testZkClient.asyncDelete(NODE_PATH, deleteCallback);
      Assert.assertFalse(deleteCallback.isOperationDone());
      Assert.assertEquals(deleteCallback.getRc(), CONNECTIONLOSS.intValue());
      // Change the mock return code.
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      // Async retry will succeed now. Wait until the operation is successfully done and verify.
      Assert.assertTrue(waitAsyncOperation(deleteCallback, RETRY_OPS_WAIT_TIMEOUT_MS));
      Assert.assertEquals(deleteCallback.getRc(), KeeperException.Code.OK.intValue());
      Assert.assertFalse(testZkClient.exists(NODE_PATH));
      Assert.assertTrue(testZkClient.getAndResetRetryCount() >= 1);
      // Check failure metric, which should be unchanged because the operation succeeded
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.WriteAsyncFailureCounter.toString()),
          _writeFailures);
    } finally {
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  /*
   * Tests if exception is thrown during retry operation,
   * the context should be cancelled correctly.
   */
  @Test(dependsOnMethods = "testAsyncWriteRetry")
  public void testAsyncWriteRetryThrowException() throws JMException {
    MockAsyncZkClient testZkClient = new MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      // 1. Test async create retry
      ZkAsyncCallbacks.CreateCallbackHandler createCallback =
          new ZkAsyncCallbacks.CreateCallbackHandler();
      Assert.assertEquals(createCallback.getRc(), KeeperException.Code.APIERROR.intValue());

      tmpRecord.setSimpleField("test", "data");
      testZkClient.setAsyncCallRC(CONNECTIONLOSS.intValue());
      // Async set will be pending due to the mock error rc is retryable.
      testZkClient.asyncCreate(NODE_PATH, tmpRecord, CreateMode.PERSISTENT, createCallback);
      Assert.assertFalse(createCallback.isOperationDone());
      Assert.assertEquals(createCallback.getRc(), CONNECTIONLOSS.intValue());
      // Throw exception in retry
      testZkClient.setZkExceptionInRetry(true);
      // Async retry will succeed now. Wait until the operation is done and verify.
      Assert.assertTrue(waitAsyncOperation(createCallback, RETRY_OPS_WAIT_TIMEOUT_MS),
          "Async callback should have been canceled");
      Assert.assertEquals(createCallback.getRc(), CONNECTIONLOSS.intValue());
      Assert.assertTrue(testZkClient.getAndResetRetryCount() >= 1);
      // Check failure metric, which should be unchanged because the operation succeeded
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.WriteAsyncFailureCounter.toString()),
          _writeFailures);

      // Restore the state
      testZkClient.setZkExceptionInRetry(false);

      // 1. Test async set retry
      ZkAsyncCallbacks.SetDataCallbackHandler setCallback =
          new ZkAsyncCallbacks.SetDataCallbackHandler();
      Assert.assertEquals(setCallback.getRc(), KeeperException.Code.APIERROR.intValue());

      tmpRecord.setSimpleField("test", "data");
      testZkClient.setAsyncCallRC(CONNECTIONLOSS.intValue());
      // Async set will be pending due to the mock error rc is retryable.
      testZkClient.asyncSetData(NODE_PATH, tmpRecord, -1, setCallback);
      Assert.assertFalse(setCallback.isOperationDone());
      Assert.assertEquals(setCallback.getRc(), CONNECTIONLOSS.intValue());
      // Throw exception in retry
      testZkClient.setZkExceptionInRetry(true);
      // Async retry will succeed now. Wait until the operation is done and verify.
      Assert.assertTrue(waitAsyncOperation(setCallback, RETRY_OPS_WAIT_TIMEOUT_MS),
          "Async callback should have been canceled");
      Assert.assertEquals(setCallback.getRc(), CONNECTIONLOSS.intValue());
      Assert.assertTrue(testZkClient.getAndResetRetryCount() >= 1);
      // Check failure metric, which should be unchanged because the operation succeeded
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.WriteAsyncFailureCounter.toString()),
          _writeFailures);
    } finally {
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  @Test(dependsOnMethods = "testAsyncWriteRetryThrowException")
  public void testAsyncReadRetry() throws JMException {
    MockAsyncZkClient testZkClient = new MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      // 1. Test async exist check
      ZkAsyncCallbacks.ExistsCallbackHandler existsCallback =
          new ZkAsyncCallbacks.ExistsCallbackHandler();
      Assert.assertEquals(existsCallback.getRc(), KeeperException.Code.APIERROR.intValue());

      testZkClient.setAsyncCallRC(CONNECTIONLOSS.intValue());
      // Async exist check will be pending due to the mock error rc is retryable.
      testZkClient.asyncExists(NODE_PATH, existsCallback);
      Assert.assertFalse(existsCallback.isOperationDone());
      Assert.assertEquals(existsCallback.getRc(), CONNECTIONLOSS.intValue());
      // Change the mock return code.
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      // Async retry will succeed now. Wait until the operation is successfully done and verify.
      Assert.assertTrue(waitAsyncOperation(existsCallback, RETRY_OPS_WAIT_TIMEOUT_MS));
      Assert.assertEquals(existsCallback.getRc(), KeeperException.Code.OK.intValue());
      Assert.assertTrue(existsCallback._stat != null);
      Assert.assertTrue(testZkClient.getAndResetRetryCount() >= 1);
      // Check failure metric, which should be unchanged because the operation succeeded
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.ReadAsyncFailureCounter.toString()),
          _readFailures);

      // 2. Test async get
      ZkAsyncCallbacks.GetDataCallbackHandler getCallback =
          new ZkAsyncCallbacks.GetDataCallbackHandler();
      Assert.assertEquals(getCallback.getRc(), KeeperException.Code.APIERROR.intValue());

      testZkClient.setAsyncCallRC(CONNECTIONLOSS.intValue());
      // Async get will be pending due to the mock error rc is retryable.
      testZkClient.asyncGetData(NODE_PATH, getCallback);
      Assert.assertFalse(getCallback.isOperationDone());
      Assert.assertEquals(getCallback.getRc(), CONNECTIONLOSS.intValue());
      // Change the mock return code.
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      // Async retry will succeed now. Wait until the operation is successfully done and verify.
      Assert.assertTrue(waitAsyncOperation(getCallback, RETRY_OPS_WAIT_TIMEOUT_MS));
      Assert.assertEquals(getCallback.getRc(), KeeperException.Code.OK.intValue());
      ZNRecord record = testZkClient.deserialize(getCallback._data, NODE_PATH);
      Assert.assertEquals(record.getSimpleField("foo"), "bar");
      Assert.assertTrue(testZkClient.getAndResetRetryCount() >= 1);
      // Check failure metric, which should be unchanged because the operation succeeded
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.ReadAsyncFailureCounter.toString()),
          _readFailures);
    } finally {
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  @Test(dependsOnMethods = "testAsyncReadRetry")
  public void testAsyncRequestCleanup() throws JMException {
    int cbCount = 10;
    MockAsyncZkClient testZkClient = new MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      // Create 10 async exists check requests
      ZkAsyncCallbacks.ExistsCallbackHandler[] existsCallbacks =
          new ZkAsyncCallbacks.ExistsCallbackHandler[cbCount];
      for (int i = 0; i < cbCount; i++) {
        existsCallbacks[i] = new ZkAsyncCallbacks.ExistsCallbackHandler();
      }
      testZkClient.setAsyncCallRC(CONNECTIONLOSS.intValue());
      // All async exist check calls will be pending due to the mock error rc is retryable.
      for (ZkAsyncCallbacks.ExistsCallbackHandler cb : existsCallbacks) {
        testZkClient.asyncExists(NODE_PATH, cb);
        Assert.assertEquals(cb.getRc(), CONNECTIONLOSS.intValue());
      }
      // Wait for a while, no callback finishes
      Assert.assertFalse(waitAsyncOperation(existsCallbacks[0], RETRY_OPS_WAIT_TIMEOUT_MS));
      for (ZkAsyncCallbacks.ExistsCallbackHandler cb : existsCallbacks) {
        Assert.assertEquals(cb.getRc(), CONNECTIONLOSS.intValue());
        Assert.assertFalse(cb.isOperationDone());
      }
      testZkClient.close();
      // All callback retry will be cancelled because the zkclient is closed.
      for (ZkAsyncCallbacks.ExistsCallbackHandler cb : existsCallbacks) {
        Assert.assertTrue(waitAsyncOperation(cb, RETRY_OPS_WAIT_TIMEOUT_MS));
        Assert.assertEquals(cb.getRc(), CONNECTIONLOSS.intValue());
        // The failure metric doesn't increase here, because an exception is thrown before the logic
        // responsible for increasing the metric is reached.
        Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
            ZkClientPathMonitor.PredefinedMetricDomains.ReadAsyncFailureCounter.toString()),
            _readFailures);
      }
      Assert.assertTrue(testZkClient.getAndResetRetryCount() >= 1);
    } finally {
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  @Test(dependsOnMethods = "testAsyncRequestCleanup")
  public void testAsyncFailureMetrics() throws JMException {
    // The remaining failure paths that weren't covered in other test methods are tested here
    MockAsyncZkClient testZkClient = new MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      // Test asyncGet failure
      ZkAsyncCallbacks.GetDataCallbackHandler getCallback =
          new ZkAsyncCallbacks.GetDataCallbackHandler();
      Assert.assertEquals(getCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      // asyncGet should fail because the return code is APIERROR
      testZkClient.setAsyncCallRC(KeeperException.Code.APIERROR.intValue());
      testZkClient.asyncGetData(NODE_PATH, getCallback);
      getCallback.waitForSuccess();
      Assert.assertEquals(getCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      ++_readFailures;
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.ReadAsyncFailureCounter.toString()),
          _readFailures);
      // asyncGet should succeed because the return code is NONODE
      testZkClient.setAsyncCallRC(KeeperException.Code.NONODE.intValue());
      testZkClient.asyncGetData(NODE_PATH, getCallback);
      getCallback.waitForSuccess();
      Assert.assertEquals(getCallback.getRc(), KeeperException.Code.NONODE.intValue());
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.ReadAsyncFailureCounter.toString()),
          _readFailures);

      // Test asyncExists failure
      ZkAsyncCallbacks.ExistsCallbackHandler existsCallback =
          new ZkAsyncCallbacks.ExistsCallbackHandler();
      Assert.assertEquals(existsCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      // asyncExists should fail because the return code is APIERROR
      testZkClient.setAsyncCallRC(KeeperException.Code.APIERROR.intValue());
      testZkClient.asyncExists(NODE_PATH, existsCallback);
      existsCallback.waitForSuccess();
      Assert.assertEquals(existsCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      ++_readFailures;
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.ReadAsyncFailureCounter.toString()),
          _readFailures);
      // asyncExists should fail because the return code is NONODE
      testZkClient.setAsyncCallRC(KeeperException.Code.NONODE.intValue());
      testZkClient.asyncExists(NODE_PATH, existsCallback);
      existsCallback.waitForSuccess();
      Assert.assertEquals(existsCallback.getRc(), KeeperException.Code.NONODE.intValue());
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.ReadAsyncFailureCounter.toString()),
          _readFailures);

      // Test asyncSet failure
      ZkAsyncCallbacks.SetDataCallbackHandler setCallback =
          new ZkAsyncCallbacks.SetDataCallbackHandler();
      Assert.assertEquals(setCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      // asyncSet should fail because the return code is APIERROR
      testZkClient.setAsyncCallRC(KeeperException.Code.APIERROR.intValue());
      testZkClient.asyncSetData(NODE_PATH, tmpRecord, -1, setCallback);
      setCallback.waitForSuccess();
      Assert.assertEquals(setCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      ++_writeFailures;
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.WriteAsyncFailureCounter.toString()),
          _writeFailures);

      // Test asyncDelete failure
      ZkAsyncCallbacks.DeleteCallbackHandler deleteCallback =
          new ZkAsyncCallbacks.DeleteCallbackHandler();
      Assert.assertEquals(deleteCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      // asyncDelete should fail because the return code is APIERROR
      testZkClient.setAsyncCallRC(KeeperException.Code.APIERROR.intValue());
      testZkClient.asyncDelete(NODE_PATH, deleteCallback);
      deleteCallback.waitForSuccess();
      Assert.assertEquals(deleteCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      ++_writeFailures;
      Assert.assertEquals((long) _beanServer.getAttribute(_rootName,
          ZkClientPathMonitor.PredefinedMetricDomains.WriteAsyncFailureCounter.toString()),
          _writeFailures);
    } finally {
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  /**
   * Mock client to whitebox test async functionality.
   */
  class MockAsyncZkClient extends ZkClient {
    private static final long RETRY_INTERVAL_MS = 500;
    private long _retryCount = 0;

    /**
     * If the specified return code is OK, call the real function.
     * Otherwise, trigger the callback with the specified RC without triggering the real ZK call.
     */
    private int _asyncCallRetCode = KeeperException.Code.OK.intValue();

    private boolean _zkExceptionInRetry = false;

    public MockAsyncZkClient(String zkAddress) {
      super(zkAddress);
      setZkSerializer(new ZNRecordSerializer());
    }

    public void setAsyncCallRC(int rc) {
      _asyncCallRetCode = rc;
    }

    public long getAndResetRetryCount() {
      long tmpCount = _retryCount;
      _retryCount = 0;
      return tmpCount;
    }

    public void setZkExceptionInRetry(boolean zkExceptionInRetry) {
      _zkExceptionInRetry = zkExceptionInRetry;
    }

    @Override
    public void asyncCreate(String path, Object datat, CreateMode mode,
        ZkAsyncCallbacks.CreateCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncCreate(path, datat, mode, cb);
        return;
      } else if (needRetry(_asyncCallRetCode)) {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, false) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncCreate(path, datat, mode, cb);
              }
            }, null);
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncCallMonitorContext(_monitor, 0, 0, false), null);
      }
    }

    @Override
    public void asyncSetData(String path, Object datat, int version,
        ZkAsyncCallbacks.SetDataCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncSetData(path, datat, version, cb);
        return;
      } else if (needRetry(_asyncCallRetCode)) {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, false) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncSetData(path, datat, version, cb);
              }
            }, null);
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncCallMonitorContext(_monitor, 0, 0, false), null);
      }
    }

    @Override
    public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncGetData(path, cb);
        return;
      } else if (needRetry(_asyncCallRetCode)) {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, true) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncGetData(path, cb);
              }
            }, null, null);
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncCallMonitorContext(_monitor, 0, 0, true), null, null);
      }
    }

    @Override
    public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncExists(path, cb);
        return;
      } else if (needRetry(_asyncCallRetCode)) {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, true) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncExists(path, cb);
              }
            }, null);
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncCallMonitorContext(_monitor, 0, 0, true), null);
      }
    }

    @Override
    public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncDelete(path, cb);
        return;
      } else if (needRetry(_asyncCallRetCode)) {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, false) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncDelete(path, cb);
              }
            });
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncCallMonitorContext(_monitor, 0, 0, false));
      }
    }

    private void preProcess() {
      _retryCount++;
      if (_zkExceptionInRetry) {
        throw new ZkException();
      }
      try {
        Thread.sleep(RETRY_INTERVAL_MS);
      } catch (InterruptedException e) {
        throw new ZkInterruptedException(e);
      }
    }
  }
}
