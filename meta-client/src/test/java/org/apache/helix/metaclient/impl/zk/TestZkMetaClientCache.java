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


import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkMetaClientCache extends ZkMetaClientTestBase {
    private static final String PATH = "/Cache";

    @Test
    public void testCreateClient() {
        final String key = "/TestZkMetaClient_testCreate";
        try (ZkMetaClient<String> zkMetaClientCache = createZkMetaClientCache()) {
            zkMetaClientCache.connect();
            // Perform some random non-read operation
            zkMetaClientCache.create(key, ENTRY_STRING_VALUE);

            try {
                //Perform some read operation - should fail.
                // TODO: Remove this once implemented.
                zkMetaClientCache.get(PATH);
                Assert.fail("Should have failed with non implemented yet.");
            } catch (Exception ignored) {
            }
        }
    }

    protected static ZkMetaClientCache<String> createZkMetaClientCache() {
        ZkMetaClientConfig config =
                new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR)
                        //.setZkSerializer(new TestStringSerializer())
                        .build();
        return new ZkMetaClientCache<>(config, TestZkMetaClientCache.PATH, true, true, true);
    }
}
