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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZkMetaClientCache<T> extends ZkMetaClient<T> implements MetaClientCacheInterface<T> {

    private ConcurrentHashMap<String, T> _dataCacheMap;
    private final String _rootEntry;
    private TrieNode _childrenCacheTree;
    private ChildChangeListener _eventListener;
    private boolean _cacheData;
    private boolean _cacheChildren;
    private static final Logger LOG = LoggerFactory.getLogger(ZkMetaClientCache.class);
    private  ZkClient _cacheClient;
    private ExecutorService executor;

    // TODO: Look into using conditional variable instead of latch.
    private final CountDownLatch _initializedCache = new CountDownLatch(1);

    /**
     * Constructor for ZkMetaClientCache.
     * @param config ZkMetaClientConfig
     * @param cacheConfig MetaClientCacheConfig
     */
    public ZkMetaClientCache(ZkMetaClientConfig config, MetaClientCacheConfig cacheConfig) {
        super(config);
        _cacheClient = getZkClient();
        _rootEntry = cacheConfig.getRootEntry();
        _cacheData = cacheConfig.getCacheData();
        _cacheChildren = cacheConfig.getCacheChildren();

        if (_cacheData) {
            _dataCacheMap = new ConcurrentHashMap<>();
        }
        if (_cacheChildren) {
            _childrenCacheTree = new TrieNode(_rootEntry, null);
        }
    }

    /**
     * Get data for a given key.
     * If datacache is enabled, will fetch for cache. If it doesn't exist
     * returns null (for when initial populating cache is in progress).
     * @param key key to identify the entry
     * @return data for the key
     */
    @Override
    public T get(final String key) {
        if (_cacheData) {
            T data = getDataCacheMap().get(key);
            if (data == null) {
                LOG.debug("Data not found in cache for key: {}. This could be because the cache is still being populated.", key);
            }
            return data;
        }
        return super.get(key);
    }

    @Override
    public List<T> get(List<String> keys) {
        List<T> dataList = new ArrayList<>();
        for (String key : keys) {
            dataList.add(get(key));
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

    private void populateAllCache() {
        // TODO: Concurrently populate children and data cache.
        if (_cacheData) {
            try {
                List<String> children = _cacheClient.getChildren(_rootEntry);
                for (String child : children) {
                    String childPath = _rootEntry + "/" + child;
                    T dataRecord = _cacheClient.readData(childPath, true);
                    getDataCacheMap().put(childPath, dataRecord);
                }
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    private class CacheUpdateRunnable implements Runnable {
        private final String path;
        private final ChildChangeListener.ChangeType changeType;

        public CacheUpdateRunnable(String path, ChildChangeListener.ChangeType changeType) {
            this.path = path;
            this.changeType = changeType;
        }

        @Override
        public void run() {
            waitForPopulateAllCache();
            //  TODO: HANDLE DEDUP EVENT CHANGES
            switch (changeType) {
                case ENTRY_CREATED:
                    // Not implemented yet.
                    modifyDataInCache(path, false);
                    break;
                case ENTRY_DELETED:
                    // Not implemented yet.
                    modifyDataInCache(path, true);
                    break;
                case ENTRY_DATA_CHANGE:
                    modifyDataInCache(path, false);
                    break;
                default:
                    LOG.error("Unknown change type: " + changeType);
            }
        }
    }

    private void waitForPopulateAllCache() {
        try {
            _initializedCache.await();
        } catch (InterruptedException e) {
            throw new MetaClientException("Interrupted while waiting for cache to populate.", e);
        }
    }

    private void modifyDataInCache(String path, Boolean isDelete) {
        if (_cacheData) {
            if (isDelete) {
                getDataCacheMap().remove(path);
            } else {
                T dataRecord = _cacheClient.readData(path, true);
                getDataCacheMap().put(path, dataRecord);
            }
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
        _eventListener = (path, changeType) -> {
            Runnable cacheUpdateRunnable = new CacheUpdateRunnable(path, changeType);
            executor.execute(cacheUpdateRunnable);
        };
        executor = Executors.newSingleThreadExecutor();
        _cacheClient.subscribePersistRecursiveListener(_rootEntry, new ChildListenerAdapter(_eventListener));
        populateAllCache();
        // Notify the latch that cache is populated.
        _initializedCache.countDown();
    }
}
