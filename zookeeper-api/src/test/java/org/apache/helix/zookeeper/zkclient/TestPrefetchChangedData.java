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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.helix.zookeeper.impl.TestHelper;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.annotation.PreFetchChangedData;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPrefetchChangedData extends ZkTestBase {
  @Test
  public void testPrefetchChangedDataEnabled() throws InterruptedException {
    String path = "/" + TestHelper.getTestMethodName();
    ZkClient zkClient = new ZkClient(ZkTestBase.ZK_ADDR);

    try {
      zkClient.createPersistent(path, "v1");

      CountDownLatch countDownLatch = new CountDownLatch(1);
      PreFetchZkDataListener dataListener = new PreFetchZkDataListener(countDownLatch);
      zkClient.subscribeDataChanges(path, dataListener);
      zkClient.writeData(path, "v2");

      Assert.assertTrue(countDownLatch.await(3L, TimeUnit.SECONDS));

      Assert.assertTrue(dataListener.isDataPreFetched());
    } finally {
      zkClient.unsubscribeAll();
      zkClient.delete(path);
      zkClient.close();
    }
  }

  @Test
  public void testPrefetchChangedDataDisabled() throws InterruptedException {
    String path = "/" + TestHelper.getTestMethodName();
    ZkClient zkClient = new ZkClient(ZkTestBase.ZK_ADDR);

    try {
      zkClient.createPersistent(path, "v1");

      CountDownLatch countDownLatch = new CountDownLatch(1);
      PreFetchZkDataListener dataListener = new PreFetchDisabledZkDataListener(countDownLatch);
      zkClient.subscribeDataChanges(path, dataListener);
      zkClient.writeData(path, "v2");

      Assert.assertTrue(countDownLatch.await(3L, TimeUnit.SECONDS));

      Assert.assertFalse(dataListener.isDataPreFetched());
    } finally {
      zkClient.unsubscribeAll();
      zkClient.delete(path);
      zkClient.close();
    }
  }

  private static class PreFetchZkDataListener implements IZkDataListener {
    private boolean isDataPreFetched;
    private CountDownLatch countDownLatch;

    public PreFetchZkDataListener(CountDownLatch countDownLatch) {
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      isDataPreFetched = (data != null);
      countDownLatch.countDown();
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // Not implemented
    }

    public boolean isDataPreFetched() {
      return isDataPreFetched;
    }
  }

  @PreFetchChangedData(enabled = false)
  private static class PreFetchDisabledZkDataListener extends PreFetchZkDataListener {
    public PreFetchDisabledZkDataListener(CountDownLatch countDownLatch) {
      super(countDownLatch);
    }
  }
}
