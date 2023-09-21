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

import org.apache.helix.metaclient.factories.MetaClientCacheConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class TestZkMetaClientCache extends ZkMetaClientTestBase {
    private static final String DATA_PATH = "/data";
    private static final String DATA_VALUE = "testData";

    @Test
    public void testCreateClient() {
        final String key = "/testCreate";
        try (ZkMetaClientCache<String> zkMetaClientCache = createZkMetaClientCacheLazyCaching(key)) {
            zkMetaClientCache.connect();
            // Perform some random non-read operation
            zkMetaClientCache.create(key, ENTRY_STRING_VALUE);
        }
    }

    @Test
    public void testLazyDataCacheAndFetch() {
        final String key = "/testLazyDataCacheAndFetch";
        try (ZkMetaClientCache<String> zkMetaClientCache = createZkMetaClientCacheLazyCaching(key)) {
            zkMetaClientCache.connect();
            zkMetaClientCache.create(key, "test");

            // Verify that data is not cached initially
            Assert.assertFalse(zkMetaClientCache.getDataCacheMap().containsKey(key + DATA_PATH));

            zkMetaClientCache.create(key + DATA_PATH, DATA_VALUE);

            // Get data for DATA_PATH (should trigger lazy loading)
            String data = zkMetaClientCache.get(key + DATA_PATH);

            // Verify that data is now cached
            Assert.assertTrue(zkMetaClientCache.getDataCacheMap().containsKey(key + DATA_PATH));
            Assert.assertEquals(DATA_VALUE, data);
        }
    }

    @Test
    public void testCacheDataUpdates() {
        final String key = "/testCacheDataUpdates";
        try (ZkMetaClientCache<String> zkMetaClientCache = createZkMetaClientCacheLazyCaching(key)) {
            zkMetaClientCache.connect();
            zkMetaClientCache.create(key, "test");
            zkMetaClientCache.create(key + DATA_PATH, DATA_VALUE);

            // Get data for DATA_PATH and cache it
            String data = zkMetaClientCache.get(key + DATA_PATH);
            Assert.assertEquals(data, zkMetaClientCache.getDataCacheMap().get(key + DATA_PATH));

            // Update data for DATA_PATH
            String newData = zkMetaClientCache.update(key + DATA_PATH, currentData -> currentData + "1");

            // Verify that cached data is updated. Might take some time
            for (int i = 0; i < 10; i++) {
                if (zkMetaClientCache.getDataCacheMap().get(key + DATA_PATH).equals(newData)) {
                    break;
                }
                Thread.sleep(1000);
            }
            Assert.assertEquals(newData, zkMetaClientCache.getDataCacheMap().get(key + DATA_PATH));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testBatchGet() {
        final String key = "/testBatchGet";
        try (ZkMetaClientCache<String> zkMetaClientCache = createZkMetaClientCacheLazyCaching(key)) {
            zkMetaClientCache.connect();
            zkMetaClientCache.create(key, "test");
            zkMetaClientCache.create(key + DATA_PATH, DATA_VALUE);

            ArrayList<String> keys = new ArrayList<>();
            keys.add(key);
            keys.add(key + DATA_PATH);

            ArrayList<String> values = new ArrayList<>();
            values.add("test");
            values.add(DATA_VALUE);

            // Get data for DATA_PATH and cache it
            List<String> data = zkMetaClientCache.get(keys);
            Assert.assertEquals(data, values);
        }
    }

    protected static ZkMetaClientCache<String> createZkMetaClientCacheLazyCaching(String rootPath) {
        ZkMetaClientConfig config =
                new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR)
                        //.setZkSerializer(new TestStringSerializer())
                        .build();
        MetaClientCacheConfig cacheConfig = new MetaClientCacheConfig(rootPath, true, true, true);
        return new ZkMetaClientCache<>(config, cacheConfig);
    }
}
