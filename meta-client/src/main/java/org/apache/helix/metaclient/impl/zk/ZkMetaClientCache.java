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
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.factories.MetaClientCacheConfig;
import org.apache.helix.metaclient.impl.zk.adapter.ChildListenerAdapter;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZkMetaClientCache<T> extends ZkMetaClient<T> implements MetaClientCacheInterface<T> {

    private ConcurrentHashMap<String, T> _dataCacheMap;
    private final String _rootEntry;
    private TrieNode _childrenCacheTree;
    private ChildChangeListener _eventListener;
    private boolean _cacheData;
    private boolean _cacheChildren;
    private boolean _lazyCaching;
    private static final Logger LOG = LoggerFactory.getLogger(ZkMetaClientCache.class);
    private  ZkClient _cacheClient;

    private ExecutorService executor;

    /**
     * Constructor for ZkMetaClientCache.
     * @param config ZkMetaClientConfig
     * @param cacheConfig MetaClientCacheConfig
     */
    public ZkMetaClientCache(ZkMetaClientConfig config, MetaClientCacheConfig cacheConfig) {
        super(config);
        _cacheClient = getZkClient();
        _rootEntry = cacheConfig.getRootEntry();
        _lazyCaching = cacheConfig.getLazyCaching();
        _cacheData = cacheConfig.getCacheData();
        _cacheChildren = cacheConfig.getCacheChildren();

        if (_cacheData) {
            _dataCacheMap = new ConcurrentHashMap<>();
        }
        if (_cacheChildren) {
            _childrenCacheTree = new TrieNode(_rootEntry, null);
        }
    }

    @Override
    public T get(final String key) {
        if (_cacheData) {
            getDataCacheMap().computeIfAbsent(key, k -> _cacheClient.readData(k, true));
            return getDataCacheMap().get(key);
        }
        return _cacheClient.readData(key, true);
    }

    @Override
    public List<T> get(List<String> keys) {
        List<T> dataList = new ArrayList<>();
        if (_cacheData) {
            for (String key : keys) {
                getDataCacheMap().computeIfAbsent(key, k -> _cacheClient.readData(k, true));
                dataList.add(getDataCacheMap().get(key));
            }
        }
        else {
            for (String key : keys) {
                dataList.add(_cacheClient.readData(key, true));
            }
        }
        return dataList;
    }

    @Override
    public List<String> getDirectChildrenKeys(final String key) {
        throw new MetaClientException("Not implemented yet.");
    }

    @Override
    public int countDirectChildren(final String key) {
        throw new MetaClientException("Not implemented yet.");
    }

    private void handleCacheUpdate(String path, ChildChangeListener.ChangeType changeType) {
        switch (changeType) {
            case ENTRY_CREATED:
                // Not implemented yet.
                break;
            case ENTRY_DELETED:
                // Not implemented yet.
                break;
            case ENTRY_DATA_CHANGE:
                modifyDataInCache(path);
                break;
            default:
                LOG.error("Unknown change type: " + changeType);
        }
    }

    private void modifyDataInCache(String path) {
        if (_cacheData) {
            T dataRecord = _cacheClient.readData(path, true);
            getDataCacheMap().put(path, dataRecord);
        }
    }

    public ConcurrentHashMap<String, T> getDataCacheMap() {
        return _dataCacheMap;
    }

    public TrieNode getChildrenCacheTree() {
        return _childrenCacheTree;
    }

    /**
     * Connect to the underlying ZkClient.
     */
    @Override
    public void connect() {
        super.connect();
        _eventListener = this::handleCacheUpdate;
        executor = Executors.newSingleThreadExecutor(); // Create a single-thread executor
        executor.execute(() -> {
            _cacheClient.subscribePersistRecursiveListener(_rootEntry, new ChildListenerAdapter(_eventListener));
        });
    }
}
