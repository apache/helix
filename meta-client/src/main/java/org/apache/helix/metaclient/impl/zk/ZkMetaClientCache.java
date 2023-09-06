package org.apache.helix.metaclient.impl.zk;

import org.apache.helix.metaclient.api.ChildChangeListener;
import org.apache.helix.metaclient.api.MetaClientCacheInterface;
import org.apache.helix.metaclient.datamodel.DataRecord;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;

import java.util.List;
import java.util.Map;

public class ZkMetaClientCache<T> extends ZkMetaClient<T> implements MetaClientCacheInterface<T> {

    private Map<String, DataRecord> _dataCacheMap;
    private final String _rootEntry;
    private TrieNode _childrenCacheTree;
    private ChildChangeListener _eventListener;
    private CacheState _cacheState;
    private boolean _cacheData;
    private boolean _cacheChildren;
    private boolean _lazyLoading;

    /**
     * Creates a cached metaclient with ZooKeeper as the underlying data storage.
     * @param config Configuration file to setup a zkmetaclient
     * @param key The root node of the entry to be cached.
     */
    public ZkMetaClientCache(ZkMetaClientConfig config, String key) {
        super(config);
        _rootEntry = key;
        _lazyLoading = true;
        _cacheData = false;
        _cacheChildren = false;
    }

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
        _rootEntry = key;
        _lazyLoading = lazyCaching;
        _cacheData = cacheData;
        _cacheChildren = cacheChildren;
    }


    private void setLazyLoading(boolean lazyLoading) {
        _lazyLoading = lazyLoading;
    }

    @Override
    public void startCache() {
        throw new MetaClientException("Not implemented yet.");
    }

    @Override
    public void closeCache() {
        throw new MetaClientException("Not implemented yet.");
    }

    @Override
    public void rebuildCache() {
        throw new MetaClientException("Not implemented yet.");
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
}