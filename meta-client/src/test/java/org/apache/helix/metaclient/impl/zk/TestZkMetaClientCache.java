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

import org.apache.helix.metaclient.MetaClientTestUtil;
import org.apache.helix.metaclient.factories.MetaClientCacheConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

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
    public void testCacheDataUpdates() {
        final String key = "/testCacheDataUpdates";
        try (ZkMetaClientCache<String> zkMetaClientCache = createZkMetaClientCacheLazyCaching(key)) {
            zkMetaClientCache.connect();
            zkMetaClientCache.create(key, "test");
            zkMetaClientCache.create(key + DATA_PATH, DATA_VALUE);
            // Get data for DATA_PATH and cache it
            Assert.assertTrue(MetaClientTestUtil.verify(() ->
                (Objects.equals(zkMetaClientCache.get(key+DATA_PATH), DATA_VALUE)), MetaClientTestUtil.WAIT_DURATION));
            Assert.assertTrue(MetaClientTestUtil.verify(() ->
                (Objects.equals(zkMetaClientCache.getDataCacheMap().get(key + DATA_PATH), DATA_VALUE)), MetaClientTestUtil.WAIT_DURATION));

            // Update data for DATA_PATH
            String newData = zkMetaClientCache.update(key + DATA_PATH, currentData -> currentData + "1");

            Assert.assertTrue(MetaClientTestUtil.verify(() ->
                    (Objects.equals(zkMetaClientCache.getDataCacheMap().get(key + DATA_PATH), newData)), MetaClientTestUtil.WAIT_DURATION));

            zkMetaClientCache.delete(key + DATA_PATH);
            Assert.assertTrue(MetaClientTestUtil.verify(() ->
                    (Objects.equals(zkMetaClientCache.getDataCacheMap().get(key + DATA_PATH), null)), MetaClientTestUtil.WAIT_DURATION));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetDirectChildrenKeys() {
        final String key = "/testGetDirectChildrenKeys";
        try (ZkMetaClientCache<String> zkMetaClientCache = createZkMetaClientCacheLazyCaching(key)) {
            zkMetaClientCache.connect();
            zkMetaClientCache.create(key, ENTRY_STRING_VALUE);
            zkMetaClientCache.create(key + "/child1", ENTRY_STRING_VALUE);
            zkMetaClientCache.create(key + "/child2", ENTRY_STRING_VALUE);

            Assert.assertTrue(MetaClientTestUtil.verify(() ->
                    (zkMetaClientCache.getDirectChildrenKeys(key).size() == 2), MetaClientTestUtil.WAIT_DURATION));

            Assert.assertTrue(zkMetaClientCache.getDirectChildrenKeys(key).contains("child1"));
            Assert.assertTrue(zkMetaClientCache.getDirectChildrenKeys(key).contains("child2"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCountDirectChildren() {
        final String key = "/testCountDirectChildren";
        try (ZkMetaClientCache<String> zkMetaClientCache = createZkMetaClientCacheLazyCaching(key)) {
            zkMetaClientCache.connect();
            zkMetaClientCache.create(key, ENTRY_STRING_VALUE);
            zkMetaClientCache.create(key + "/child1", ENTRY_STRING_VALUE);
            zkMetaClientCache.create(key + "/child2", ENTRY_STRING_VALUE);
            Assert.assertTrue(MetaClientTestUtil.verify(() -> ( zkMetaClientCache.countDirectChildren(key) == 2), MetaClientTestUtil.WAIT_DURATION));
        } catch (Exception e) {
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

            Assert.assertTrue(MetaClientTestUtil.verify(() -> ( zkMetaClientCache.get(keys).equals(values)), MetaClientTestUtil.WAIT_DURATION));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLargeClusterLoading() {
        final String key = "/testLargerNodes";
        try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
            zkMetaClient.connect();

            int numLayers = 4;
            int numNodesPerLayer = 20;

            // Create the root node
            zkMetaClient.create(key, "test");

            Queue<String> queue = new LinkedList<>();
            queue.offer(key);

            for (int layer = 1; layer <= numLayers; layer++) {
                int nodesAtThisLayer = Math.min(numNodesPerLayer, queue.size() * numNodesPerLayer);

                for (int i = 0; i < nodesAtThisLayer; i++) {
                    String parentKey = queue.poll();
                    for (int j = 0; j < numNodesPerLayer; j++) {
                        String newNodeKey = parentKey + "/node" + j;
                        zkMetaClient.create(newNodeKey, "test");
                        queue.offer(newNodeKey);
                    }
                }
            }

            try (ZkMetaClientCache<String> zkMetaClientCache = createZkMetaClientCacheLazyCaching(key)) {
                zkMetaClientCache.connect();

                // Assert Checks on a Random Path
                Assert.assertTrue(MetaClientTestUtil.verify(() -> ( zkMetaClientCache.get(key + "/node4/node1").equals("test")), MetaClientTestUtil.WAIT_DURATION));
                Assert.assertTrue(MetaClientTestUtil.verify(() -> ( zkMetaClientCache.countDirectChildren(key) == numNodesPerLayer), MetaClientTestUtil.WAIT_DURATION));
                String newData = zkMetaClientCache.update(key + "/node4/node1", currentData -> currentData + "1");
                Assert.assertTrue(MetaClientTestUtil.verify(() -> ( zkMetaClientCache.get(key + "/node4/node1").equals(newData)), MetaClientTestUtil.WAIT_DURATION));
                Assert.assertTrue(MetaClientTestUtil.verify(() ->
                        (zkMetaClientCache.getDirectChildrenKeys(key + "/node4/node1")
                                .equals(zkMetaClient.getDirectChildrenKeys(key + "/node4/node1"))), MetaClientTestUtil.WAIT_DURATION));

                zkMetaClientCache.delete(key + "/node4/node1");
                Assert.assertTrue(MetaClientTestUtil.verify(() -> (Objects.equals(zkMetaClientCache.get(key + "/node4/node1"), null)), MetaClientTestUtil.WAIT_DURATION));

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    public ZkMetaClientCache<String> createZkMetaClientCacheLazyCaching(String rootPath) {
        ZkMetaClientConfig config =
                new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR)
                        //.setZkSerializer(new TestStringSerializer())
                        .build();
        MetaClientCacheConfig cacheConfig = new MetaClientCacheConfig(rootPath, true, true);
        return new ZkMetaClientCache<>(config, cacheConfig);
    }
}
