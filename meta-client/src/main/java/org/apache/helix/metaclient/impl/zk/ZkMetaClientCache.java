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

import org.apache.helix.metaclient.api.ChildChangeListener;
import org.apache.helix.metaclient.api.MetaClientCacheInterface;
import org.apache.helix.metaclient.datamodel.DataRecord;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientFactory;
import org.apache.helix.metaclient.recipes.lock.LockInfoSerializer;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ZkMetaClientCache<T> extends ZkMetaClient<T> implements MetaClientCacheInterface<T> {

    private Map<String, DataRecord> _dataCacheMap;
    private final String _rootEntry;
    private TrieNode _childrenCacheTree;
    private ChildChangeListener _eventListener;
    private boolean _cacheData;
    private boolean _cacheChildren;
    private boolean _lazyCaching;
    private static final Logger LOG = LoggerFactory.getLogger(ZkMetaClientCache.class);
    private  ZkClient _cacheClient;

    /**
     * Creates a cached metaclient with ZooKeeper as the underlying data storage.
     * @param config Configuration file to setup a zkmetaclient
     * @param key The root node of the entry to be cached.
     * @param cacheData Indicates whether the data should be cached.
     * @param cacheChildren Indicates whether the children of the nodes should be cached.
     * @param lazyCaching Indicates whether lazy loading of the cache is enabled.
     */
    public ZkMetaClientCache(ZkMetaClientConfig config, String key, Boolean cacheData,
                             Boolean cacheChildren, Boolean lazyCaching) {
        super(config);
        _cacheClient = getZkClient();
        _rootEntry = key;
        _lazyCaching = lazyCaching;
        _cacheData = cacheData;
        _cacheChildren = cacheChildren;
    }

    private void setLazyLoading(boolean lazyLoading) {
        _lazyCaching = lazyLoading;
    }

    @Override
    public Stat exists(String key) {
        throw new MetaClientException("Not implemented yet.");
    }

    @Override
    public T get(final String key) {
        throw new MetaClientException("Not implemented yet.");
    }

    @Override
    public List<String> getDirectChildrenKeys(final String key) {
        throw new MetaClientException("Not implemented yet.");
    }

    @Override
    public int countDirectChildren(final String key) {
        throw new MetaClientException("Not implemented yet.");
    }

    @Override
    public List<T> get(List<String> keys) {
        throw new MetaClientException("Not implemented yet.");
    }

    @Override
    public List<Stat> exists(List<String> keys) {
        throw new MetaClientException("Not implemented yet.");
    }

    public ZkClient getCacheClient() {
        return _cacheClient;
    }
}