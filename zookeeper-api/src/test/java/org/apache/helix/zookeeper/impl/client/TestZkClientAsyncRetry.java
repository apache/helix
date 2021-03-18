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

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncRetryCallContext;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks.UNKNOWN_RET_CODE;
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

  private org.apache.helix.zookeeper.zkclient.ZkClient _zkClient;
  private String _zkServerAddress;

  @BeforeClass
  public void beforeClass() {
    _zkClient = _zkServerMap.values().iterator().next().getZkClient();
    _zkServerAddress = _zkClient.getServers();
    _zkClient.createPersistent(TEST_ROOT);
  }

  @AfterClass
  public void afterClass() {
    _zkClient.deleteRecursively(TEST_ROOT);
    _zkClient.close();
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

  @Test
  public void testAsyncRetryCategories() {
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
        Assert.assertEquals(createCallback.getRc(), UNKNOWN_RET_CODE);
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
        }
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
  public void testAsyncWriteRetry() {
    MockAsyncZkClient testZkClient = new MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      // 1. Test async set retry
      ZkAsyncCallbacks.SetDataCallbackHandler setCallback =
          new ZkAsyncCallbacks.SetDataCallbackHandler();
      Assert.assertEquals(setCallback.getRc(), UNKNOWN_RET_CODE);

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

      // 2. Test async delete
      ZkAsyncCallbacks.DeleteCallbackHandler deleteCallback =
          new ZkAsyncCallbacks.DeleteCallbackHandler();
      Assert.assertEquals(deleteCallback.getRc(), UNKNOWN_RET_CODE);

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
  public void testAsyncWriteRetryThrowException() {
    MockAsyncZkClient testZkClient = new MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      // 1. Test async create retry
      ZkAsyncCallbacks.CreateCallbackHandler createCallback =
          new ZkAsyncCallbacks.CreateCallbackHandler();
      Assert.assertEquals(createCallback.getRc(), UNKNOWN_RET_CODE);

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

      // Restore the state
      testZkClient.setZkExceptionInRetry(false);

      // 1. Test async set retry
      ZkAsyncCallbacks.SetDataCallbackHandler setCallback =
          new ZkAsyncCallbacks.SetDataCallbackHandler();
      Assert.assertEquals(setCallback.getRc(), UNKNOWN_RET_CODE);

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
    } finally {
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  @Test(dependsOnMethods = "testAsyncWriteRetryThrowException")
  public void testAsyncReadRetry() {
    MockAsyncZkClient testZkClient = new MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      // 1. Test async exist check
      ZkAsyncCallbacks.ExistsCallbackHandler existsCallback =
          new ZkAsyncCallbacks.ExistsCallbackHandler();
      Assert.assertEquals(existsCallback.getRc(), UNKNOWN_RET_CODE);

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

      // 2. Test async get
      ZkAsyncCallbacks.GetDataCallbackHandler getCallback =
          new ZkAsyncCallbacks.GetDataCallbackHandler();
      Assert.assertEquals(getCallback.getRc(), UNKNOWN_RET_CODE);

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
    } finally {
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  @Test(dependsOnMethods = "testAsyncReadRetry")
  public void testAsyncRequestCleanup() {
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
      }
      Assert.assertTrue(testZkClient.getAndResetRetryCount() >= 1);
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
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, false) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncCreate(path, datat, mode, cb);
              }
            }, null);
      }
    }

    @Override
    public void asyncSetData(String path, Object datat, int version,
        ZkAsyncCallbacks.SetDataCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncSetData(path, datat, version, cb);
        return;
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, false) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncSetData(path, datat, version, cb);
              }
            }, null);
      }
    }

    @Override
    public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncGetData(path, cb);
        return;
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, true) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncGetData(path, cb);
              }
            }, null, null);
      }
    }

    @Override
    public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncExists(path, cb);
        return;
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, true) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncExists(path, cb);
              }
            }, null);
      }
    }

    @Override
    public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncDelete(path, cb);
        return;
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, null, 0, 0, false) {
              @Override
              protected void doRetry() {
                preProcess();
                asyncDelete(path, cb);
              }
            });
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
