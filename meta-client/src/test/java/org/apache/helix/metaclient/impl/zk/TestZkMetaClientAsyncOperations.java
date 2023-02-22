package org.apache.helix.metaclient.impl.zk;

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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.helix.metaclient.api.AsyncCallback;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkMetaClientAsyncOperations extends ZkMetaClientTestBase {

  static TestAsyncContext[] asyncContext = new TestAsyncContext[1];
  static final String entryKey = "/TestAsyncEntryKey";
  static final String nonExistsEntry = "/a/b/c";
  static final long LATCH_WAIT_TIMEOUT_IN_S = 3 * 60;

  static class TestAsyncContext {
    int _asyncCallSize;
    CountDownLatch _countDownLatch;
    int[] _returnCode;
    MetaClientInterface.Stat[] _stats;
    String[] _data;

    TestAsyncContext(int callSize) {
      _asyncCallSize = callSize;
      _countDownLatch = new CountDownLatch(callSize);
      _returnCode = new int[callSize];
      _stats = new MetaClientInterface.Stat[callSize];
      _data = new String[callSize];
    }

    public CountDownLatch getCountDownLatch() {
      return _countDownLatch;
    }

    public void countDown() {
      _countDownLatch.countDown();
    }

    public int getReturnCode(int idx) {
      return _returnCode[idx];
    }

    public MetaClientInterface.Stat getStats(int idx) {
      return _stats[idx];
    }

    public String getData(int idx) {
      return _data[idx];
    }

    public void setReturnCodeWhenFinished(int idx, int returnCode) {
      _returnCode[idx] = returnCode;
    }

    public void setStatWhenFinished(int idx, MetaClientInterface.Stat stat) {
      _stats[idx] = stat;
    }

    public void setDataWhenFinished(int idx, String data) {
      _data[idx] = data;
    }
  }

  @Test
  public void testAsyncCreateSetAndGet() {
    asyncContext[0] = new TestAsyncContext(2);
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();

      zkMetaClient
          .asyncCreate(entryKey, "async_create-data", MetaClientInterface.EntryMode.PERSISTENT,
              new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int returnCode, String key) {
                  asyncContext[0].setReturnCodeWhenFinished(0, returnCode);
                  asyncContext[0].countDown();
                }
              });

      zkMetaClient.asyncCreate(nonExistsEntry, "async_create-data-invalid",
          MetaClientInterface.EntryMode.PERSISTENT, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int returnCode, String key) {
              asyncContext[0].setReturnCodeWhenFinished(1, returnCode);
              asyncContext[0].countDown();
            }
          });

      asyncContext[0].getCountDownLatch().await(LATCH_WAIT_TIMEOUT_IN_S, TimeUnit.SECONDS);

      Assert.assertEquals(asyncContext[0].getReturnCode(0), KeeperException.Code.OK.intValue());
      Assert.assertEquals(asyncContext[0].getReturnCode(1), KeeperException.Code.NONODE.intValue());

      // create the entry again and expect a duplicated error code
      asyncContext[0] = new TestAsyncContext(1);
      zkMetaClient
          .asyncCreate(entryKey, "async_create-data", MetaClientInterface.EntryMode.PERSISTENT,
              new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int returnCode, String key) {
                  asyncContext[0].setReturnCodeWhenFinished(0, returnCode);
                  asyncContext[0].countDown();
                }
              });
      asyncContext[0].getCountDownLatch().await(LATCH_WAIT_TIMEOUT_IN_S, TimeUnit.SECONDS);
      Assert.assertEquals(asyncContext[0].getReturnCode(0),
          KeeperException.Code.NODEEXISTS.intValue());


      // test set
      asyncContext[0] = new TestAsyncContext(1);
      zkMetaClient
          .asyncSet(entryKey, "async_create-data-new", 0,
              new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int returnCode, String key,
                    @Nullable MetaClientInterface.Stat stat) {
                  asyncContext[0].setReturnCodeWhenFinished(0, returnCode);
                  asyncContext[0].setStatWhenFinished(0, stat);
                  asyncContext[0].countDown();
                }
              });
      asyncContext[0].getCountDownLatch().await(LATCH_WAIT_TIMEOUT_IN_S, TimeUnit.SECONDS);
      Assert.assertEquals(asyncContext[0].getReturnCode(0),
          KeeperException.Code.OK.intValue());
      Assert.assertEquals(asyncContext[0].getStats(0).getEntryType(),
          MetaClientInterface.EntryMode.PERSISTENT);
      Assert.assertEquals(asyncContext[0].getStats(0).getVersion(), 1);

      // test get
      asyncContext[0] = new TestAsyncContext(1);
      zkMetaClient.asyncGet(entryKey, new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int returnCode, String key, byte[] data,
            MetaClientInterface.Stat stat) {
          asyncContext[0].setReturnCodeWhenFinished(0, returnCode);
          asyncContext[0].setStatWhenFinished(0, stat);
          asyncContext[0].setDataWhenFinished(0, zkMetaClient.deserialize(data, key));
          asyncContext[0].countDown();
        }
      });

      asyncContext[0].getCountDownLatch().await(LATCH_WAIT_TIMEOUT_IN_S, TimeUnit.SECONDS);

      Assert.assertEquals(asyncContext[0].getReturnCode(0), KeeperException.Code.OK.intValue());
      Assert.assertEquals(asyncContext[0].getStats(0).getEntryType(),
          MetaClientInterface.EntryMode.PERSISTENT);
      Assert.assertEquals(asyncContext[0].getStats(0).getVersion(), 1);
      Assert.assertEquals(asyncContext[0].getData(0), "async_create-data-new");
    } catch (Exception ex) {
      Assert.fail("Test testAsyncCreate failed because of:", ex);
    }
  }

  @Test(dependsOnMethods = "testAsyncCreateSetAndGet")
  public void testAsyncExistsAndDelete() {
    asyncContext[0] = new TestAsyncContext(2);
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();

      zkMetaClient.asyncExist(entryKey, new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int returnCode, String key, MetaClientInterface.Stat stat) {
          asyncContext[0].setReturnCodeWhenFinished(0, returnCode);
          asyncContext[0].setStatWhenFinished(0, stat);
          asyncContext[0].countDown();
        }
      });

      zkMetaClient.asyncExist(nonExistsEntry, new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int returnCode, String key, MetaClientInterface.Stat stat) {
          asyncContext[0].setReturnCodeWhenFinished(1, returnCode);
          asyncContext[0].setStatWhenFinished(1, stat);
          asyncContext[0].countDown();
        }
      });

      asyncContext[0].getCountDownLatch().await(LATCH_WAIT_TIMEOUT_IN_S, TimeUnit.SECONDS);

      Assert.assertEquals(asyncContext[0].getReturnCode(0), KeeperException.Code.OK.intValue());
      Assert.assertEquals(asyncContext[0].getStats(0).getEntryType(),
          MetaClientInterface.EntryMode.PERSISTENT);
      Assert.assertEquals(asyncContext[0].getStats(0).getVersion(), 1);
      Assert.assertEquals(asyncContext[0].getReturnCode(1), KeeperException.Code.NONODE.intValue());
      Assert.assertNull(asyncContext[0].getStats(1));

      // test delete
      asyncContext[0] = new TestAsyncContext(1);
      zkMetaClient.asyncDelete(entryKey, new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int returnCode, String key) {
          asyncContext[0].setReturnCodeWhenFinished(0, returnCode);
          asyncContext[0].countDown();
        }
      });

      asyncContext[0].getCountDownLatch().await(LATCH_WAIT_TIMEOUT_IN_S, TimeUnit.SECONDS);

      Assert.assertEquals(asyncContext[0].getReturnCode(0), KeeperException.Code.OK.intValue());

      // node should not be there
      Assert.assertNull(zkMetaClient.get(entryKey));
    } catch (InterruptedException ex) {
      Assert.fail("Test testAsyncCreate failed because of:", ex);
    }
  }
}
