package org.apache.helix.metaclient.api;

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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.helix.metaclient.exception.MetaClientInterruptException;
import org.apache.helix.metaclient.exception.MetaClientNoNodeException;
import org.apache.helix.metaclient.exception.MetaClientTimeoutException;


public interface MetaClientInterface<T> {

  enum EntryMode {
    // The node will be removed automatically when the session associated with the creation
    // of the node expires.
    EPHEMERAL,

    // The node will not be automatically deleted upon client's disconnect.
    // An ephemeral node cannot have sub entry.
    PERSISTENT,

    // For metadata storage that has hierarchical key space (e.g. ZK), the node will be
    // automatically deleted at some point in the future if the last child of the node is deleted.
    // For metadata storage that has non-hierarchical key space (e.g. etcd), the node will be
    // automatically deleted at some point in the future if the last entry that has the prefix
    // is deleted.
    // The node is an ephemeral node.
    CONTAINER,

    // For metadata storage that has hierarchical key space (e.g. ZK) If the entry is not modified
    // within the TTL and has no children it will become a candidate to be deleted by the server
    // at some point in the future.
    // For metadata storage that has non-hierarchical key space (e.g. etcd) If the entry is not modified
    // within the TTL, it will become a candidate to be deleted by the server at some point in the
    // future.
    TTL
  }

  enum ConnectState {
    // Client is not connected to server. Before initiating connection or after close.
    NOT_CONNECTED,

    // Client is connected to server
    CONNECTED,

    // Authentication failed.
    AUTH_FAILED,

    // Server has expired this connection.
    EXPIRED,

    // When client explicitly call disconnect.
    CLOSED_BY_CLIENT,

    // Connection between client and server is lost.
    DISCONNECTED,

    // Client is authenticated. They can perform operation with authorized permissions.
    // This state is not in use as of now.
    AUTHENTICATED
  }

  /**
   * Interface representing the metadata of an entry. It contains entry type and version number.
   * TODO: we will add session ID to entry stats in the future
   * TODO: Add support for expiry time
   */
  class Stat {
    private final int _version;
    private final EntryMode _entryMode;
    // The expiry time of a TTL node in milliseconds. The default is -1 for nodes without expiry time.
    private long _expiryTime;

    // The time when the node is created. Measured in milliseconds since the Unix epoch (January 1, 1970, 00:00:00 UTC).
    private long _creationTime;

    // The time when the node was las modified. Measured in milliseconds since the Unix epoch when the node was last modified.
    private long _modifiedTime;

    public EntryMode getEntryType() {
      return _entryMode;
    }

    public int getVersion() {
      return _version;
    }

    public long getExpiryTime() {
      return _expiryTime;
    }

    public long getCreationTime() {
      return _creationTime;
    }

    public long getModifiedTime() {
      return _modifiedTime;
    }

    public Stat (EntryMode mode, int version) {
      _version = version;
      _entryMode = mode;
      _expiryTime = -1;
    }

    public Stat (EntryMode mode, int version, long ctime, long mtime, long etime) {
      _version = version;
      _entryMode = mode;
      _creationTime = ctime;
      _modifiedTime = mtime;
      _expiryTime = etime;
    }
  }

  //synced CRUD API

  /**
   * Create a persistent entry with given key and data. The entry will not be created if there is
   * an existing entry with the same key.
   * @param key key to identify the entry
   * @param data value of the entry
   */
  void create(final String key, T data);

  /**
   * Create an entry of given EntryMode with given key and data. The entry will not be created if
   * there is an existing entry with ethe same key.
   * @param key key to identify the entry
   * @param data value of the entry
   * @param mode EntryMode identifying if the entry will be deleted upon client disconnect
   */
  void create(final String key, final T data, final EntryMode mode);

  /**
   * Create an entry of given EntryMode with given key, data, and expiry time (ttl).
   * The entry will automatically purge when reached expiry time and has no children.
   * The entry will not be created if there is an existing entry with the same key.
   * @param key key to identify the entry
   * @param data value of the entry
   * @param ttl Time-to-live value of the node in milliseconds.
   */
  void createWithTTL(final String key, final T data, final long ttl);

  /**
   * Renews the specified TTL node adding its original expiry time
   * to the current time. Throws an exception if the key is not a valid path
   * or isn't of type TTL.
   * @param key key to identify the entry
   */
  void renewTTLNode(final String key);

  /**
   * Set the data for the entry of the given key if it exists and the given version matches the
   * version of the node (if the given version is -1, it matches any node's versions).
   * @param key key to identify the entry
   * @param data new data of the entry
   * @param version expected version of the entry. -1 matched any version.
   */
  void set(final String key, final T data, int version);

  /**
   * Update existing data of a given key using an updater. This method will issue a read to get
   * current data and apply updater upon the current data.
   * @param key key to identify the entry
   * @param updater An updater that modifies the entry value.
   * @return the updated value.
   */
  T update(final String key, DataUpdater<T> updater);

  /**
   * Check if there is an entry for the given key.
   * @param key key to identify the entry
   * @return return a Stat object if the entry exists. Return null otherwise.
   */
  Stat exists(final String key);

  /**
   * Fetch the data for a given key.
   * @param key key to identify the entry
   * @return Return data of the entry. Return null if data does not exists.
   */
  T get(final String key);

  /**
   * Fetch the data and stat for a given key.
   * @param key key to identify the entry
   * @return Return an ImmutablePair of data and stat for the entry.
   * @throws MetaClientNoNodeException if no such entry
   */
  ImmutablePair<T, Stat> getDataAndStat(final String key);

  /**
   * API for transaction. The list of operation will be executed as an atomic operation.
   * @param ops a list of operations. These operations will all be executed or none of them.
   * @return Return a list of OpResult.
   */
  List<OpResult> transactionOP(final Iterable<Op> ops);

  /**
   * Return a list of children for the given keys.
   * @param key For metadata storage that has hierarchical key space (e.g. ZK), the key would be
   *            a parent key,
   *            For metadata storage that has non-hierarchical key space (e.g. etcd), the key would
   *            be a prefix key.
   * @return Return a list of children keys. Return direct child name only for hierarchical key
   *        space, return the whole sub key for non-hierarchical key space.
   */
  List<String> getDirectChildrenKeys(final String key);

  /**
   * Return the number of children for the given keys.
   * @param key For metadata storage that has hierarchical key space (e.g. ZK), the key would be
   *            a parent key,
   *            For metadata storage that has non-hierarchical key space (e.g. etcd), the key would
   *            be a prefix key.
   */
  int countDirectChildren(final String key);

  /**
   * Remove the entry associated with the given key.
   * For metadata storage that has hierarchical key space, the entry can only be deleted if the key
   * has no child entry.
   * TODO: define exception to throw
   * @param key  key to identify the entry to delete
   * @return Return true if the deletion is completed
   */
  boolean delete(final String key);

  /**
   * Remove the entry associated with the given key.
   * For metadata storage that has hierarchical key space, remove all its child entries as well
   * For metadata storage that has non-hierarchical key space, this API is the same as delete()
   * @param key key to identify the entry to delete
   * @return Return true if the deletion is completed
   */
  boolean recursiveDelete(final String key);

  /* Asynchronous methods return immediately.
   * They take a callback object that will be executed either on successful execution of the request
   * or on error with an appropriate return code indicating the error.
   */

  /**
   * User may register callbacks for async CRUD calls. These callbacks will be executed in a async
   * thread pool. User could define the thread pool size. Default value is 10.
   * TODO: add const default value in a separate file
   * @param poolSize pool size for executing user resisted async callbacks
   */
  void setAsyncExecPoolSize(int poolSize);

  /**
   * The asynchronous version of create.
   * @param key key to identify the entry
   * @param data value of the entry
   * @param mode EntryMode identifying if the entry will be deleted upon client disconnect
   * @param cb A user defined VoidCallback implementation that will be invoked when async create return.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.VoidCallback
   */
  void asyncCreate(final String key, final T data, final EntryMode mode,
      AsyncCallback.VoidCallback cb);

  /**
   * The asynchronous version of set.
   * @param key key to identify the entry
   * @param data new data of the entry
   * @param version expected version if the entry. -1 matched any version
   * @param cb A user defined VoidCallback implementation that will be invoked when async create return.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.StatCallback
   */
  void asyncSet(final String key, final T data, final int version, AsyncCallback.StatCallback cb);

  /**
   * The asynchronous version of update.
   * @param key key to identify the entry
   * @param updater An updater that modifies the entry value.
   * @param cb A user defined VoidCallback implementation that will be invoked when async create return.
   *           It will contain the newly updated data if update succeeded.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.DataCallback
   */
  void asyncUpdate(final String key, DataUpdater<T> updater, AsyncCallback.DataCallback cb);

  /**
   * The asynchronous version of get.
   * @param key key to identify the entry
   * @param cb A user defined VoidCallback implementation that will be invoked when async get return.
   *           It will contain the entry data if get succeeded.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.DataCallback
   */
  void asyncGet(final String key, AsyncCallback.DataCallback cb);

  /**
   * The asynchronous version of get sub entries.
   * @param key key to identify the entry
   * @param cb A user defined VoidCallback implementation that will be invoked when async count child return.
   *           It will contain the list of child keys if succeeded.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.DataCallback
   */
  void asyncCountChildren(final String key, AsyncCallback.DataCallback cb);

  /**
   * The asynchronous version of get sub entries.
   * @param key key to identify the entry
   * @param cb A user defined VoidCallback implementation that will be invoked when async exist return.
   *           It will contain the stats of the entry if succeeded.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.StatCallback
   */
  void asyncExist(final String key, AsyncCallback.StatCallback cb);

  /**
   * The asynchronous version of delete.
   * @param key key to identify the entry
   * @param cb A user defined VoidCallback implementation that will be invoked when async delete
   *           finish and return.  @see org.apache.helix.metaclient.api.AsyncCallback.DataCallback
   */
  void asyncDelete(final String key, AsyncCallback.VoidCallback cb);

  /**
   * The asynchronous version of transaction operations.
   * @param ops A list of operations
   * @param cb A user defined TransactionCallback implementation that will be invoked when
   *           transaction operations finish and return. The TransactionCallback will contain
   *           either a list of OpResult if transaction finish successfully, or a return code
   *           indicating failure reason. @see org.apache.helix.metaclient.api.AsyncCallback.TransactionCallback
   */
  void asyncTransaction(final Iterable<Op> ops, AsyncCallback.TransactionCallback cb);

  /* Batched APIs return result to user when all request finishes.
   * These calls are not executed as a transaction.
   */

  /**
   * Batch version of create. All entries will be created in persist mode. Returns when all request
   * finishes. These calls are not executed as a transaction.
   * @param key A list of key for create operations.
   * @param data A list of data. Need to be in the same length of list of key.
   * @return A list of boolean indicating create result of each operation.
   */
  boolean[] create(List<String> key, List<T> data);

  /**
   * Batch version of create. Returns when all request finishes. These calls are not executed as a
   * transaction.
   * @param key A list of key for create operations.
   * @param data A list of data. Need to be in the same length of list of key.
   * @param mode A list of EntryMode. Need to be in the same length of list of key.
   * @return A list of boolean indicating create result of each operation.
   */
  boolean[] create(List<String> key, List<T> data, List<EntryMode> mode);

  /**
   * Batch version of set. Returns when all request finishes. These calls are not executed as a
   * transaction.
   * @param keys A list of key for set operations.
   * @param datas A list of data. Need to be in the same length of list of key.
   * @param version A list of expected version of the entry. -1 matched any version.
   *                Need to be in the same length of list of key.
   * @return A list of boolean indicating set result of each operation.
   */
  boolean[] set(List<String> keys, List<T> datas, List<Integer> version);

  /**
   * Batch version of update. Returns when all request finishes. These calls are not executed as a
   * transaction.
   * @param keys A list of key for update operations.
   * @param updater A list of updater. Need to be in the same length of list of key.
   * @return A list of updated entry values.
   */
  List<T> update(List<String> keys, List<DataUpdater<T>> updater);

  /**
   * Batch version of get. Returns when all request finishes. These calls are not executed as a
   * transaction.
   * @param keys A list of key for get operations.
   * @return A list of entry values.
   */
  List<T> get(List<String> keys);

  /**
   * Batch version of exists. Returns when all request finishes. These calls are not executed as a
   * transaction.
   * @param keys A list of key for exists operations.
   * @return A list of stats for the given entries.
   */
  List<Stat> exists(List<String> keys);

  /**
   * Batch version of delete. Returns when all request finishes. These calls are not executed as a
   * transaction.
   * @param keys A list of key for delete operations.
   * @return A list of boolean indicating delete result of each operation.
   */
  boolean[] delete(List<String> keys);

  /**
   * Maintains a connection with underlying metadata service based on config params. Connection
   * created by this method will be used to perform CRUD operations on metadata service.
   * @throws MetaClientInterruptException
   *          if the connection timed out due to thread interruption
   * @throws MetaClientTimeoutException
   *          if the connection timed out
   * @throws IllegalStateException
   *         if already connected or the connection is already closed explicitly
   */
  void connect();

  /**
   * Disconnect from server explicitly.
   */
  void disconnect();

  /**
   * Check whether client is closed
   * @return true if client is closed, false otherwise
   */
   boolean isClosed();

  /**
   * @return client current connection state with metadata service.
   */
  ConnectState getClientConnectionState();

  // Event notification APIs, user can register multiple listeners on the same key/connection state.
  // All listeners will be automatically removed when client is disconnected.
  // TODO: add auto re-register listener option

  /**
   * Subscribe change of a particular entry. Including entry data change, entry deletion and creation
   * of the given key.
   * The listener should be permanent until it's unsubscribed.
   * @param key Key to identify the entry
   * @param listener An implementation of {@link org.apache.helix.metaclient.api.DataChangeListener} to register
   * @param skipWatchingNonExistNode Will not register lister to a non-exist key if set to true.
   *                                 Please set to false if you are expecting ENTRY_CREATED type.
   * @return Return a boolean indication if subscribe succeeded.
   */
  boolean subscribeDataChange(String key, DataChangeListener listener, boolean skipWatchingNonExistNode);

  /**
   * Subscribe a one-time change of a particular entry. Including entry data change, entry deletion and creation
   * of the given key.
   * The implementation should use at-most-once delivery semantic.
   * @param key Key to identify the entry
   * @param listener An implementation of {@link org.apache.helix.metaclient.api.DataChangeListener} to register
   * @param skipWatchingNonExistNode Will not register lister to a non-exist key if set to true.
   *                                 Please set to false if you are expecting ENTRY_CREATED type.
   * @return Return a boolean indication if subscribe succeeded.
   */
  default boolean subscribeOneTimeDataChange(String key, DataChangeListener listener,
      boolean skipWatchingNonExistNode) {
    throw new NotImplementedException("subscribeOneTimeDataChange is not implemented");
  }

  /**
   * Subscribe for direct child change event on a particular key. It includes new child
   * creation or deletion. It does not include existing child data change.
   * The listener should be permanent until it's unsubscribed.
   * For hierarchy key spaces like zookeeper, it refers to an entry's direct children nodes.
   * For flat key spaces, it refers to keys that matches `prefix*separator`.
   * @param key key to identify the entry.
   * @param listener An implementation of {@link org.apache.helix.metaclient.api.DirectChildChangeListener} to register
   * @param skipWatchingNonExistNode If the passed in key does not exist, no listener wil be registered.
   *
   * @return Return a DirectChildSubscribeResult. It will contain a list of direct sub children if
   *         subscribe succeeded.
   */
  DirectChildSubscribeResult subscribeDirectChildChange(String key,
      DirectChildChangeListener listener, boolean skipWatchingNonExistNode);

  /**
   * Subscribe for a one-time direct child change event on a particular key. It includes new child
   * creation or deletion. It does not include existing child data change.
   * The implementation should use at-most-once delivery semantic.
   * For hierarchy key spaces like zookeeper, it refers to an entry's direct children nodes.
   * For flat key spaces, it refers to keys that matches `prefix*separator`.
   *
   * @param key key to identify the entry.
   * @param listener An implementation of {@link org.apache.helix.metaclient.api.DirectChildChangeListener} to register
   * @param skipWatchingNonExistNode If the passed in key does not exist, no listener wil be registered.
   *
   * @return Return a DirectChildSubscribeResult. It will contain a list of direct sub children if
   *         subscribe succeeded.
   */
  default DirectChildSubscribeResult subscribeOneTimeDirectChildChange(String key,
      DirectChildChangeListener listener, boolean skipWatchingNonExistNode) {
    throw new NotImplementedException("subscribeOneTimeDirectChildChange is not implemented");
  }

  /**
   * Subscribe for connection state change.
   * The listener should be permanent until it's unsubscribed.
   * @param listener An implementation of {@link org.apache.helix.metaclient.api.ConnectStateChangeListener} to register
   *
   * @return Return a boolean indication if subscribe succeeded.
   */
  boolean subscribeStateChanges(ConnectStateChangeListener listener);

  /**
   * Subscribe change for all children including entry change and data change.
   * The listener should be permanent until it's unsubscribed.
   * For hierarchy key spaces like zookeeper, it would watch the whole tree structure.
   * For flat key spaces, it would watch for keys with certain prefix.
   * @param key key to identify the entry.
   * @param listener An implementation of {@link org.apache.helix.metaclient.api.ChildChangeListener} to register
   * @param skipWatchingNonExistNode If the passed in key does not exist, no listener wil be registered.
   */
  boolean subscribeChildChanges(String key, ChildChangeListener listener, boolean skipWatchingNonExistNode);

  /**
   * Subscribe a one-time change for all children including entry change and data change.
   * The implementation should use at-most-once delivery semantic.
   * For hierarchy key spaces like zookeeper, it would watch the whole tree structure.
   * For flat key spaces, it would watch for keys with certain prefix.
   * @param key key to identify the entry.
   * @param listener An implementation of {@link org.apache.helix.metaclient.api.ChildChangeListener} to register
   * @param skipWatchingNonExistNode If the passed in key does not exist, no listener wil be registered.
   */
  default boolean subscribeOneTimeChildChanges(String key, ChildChangeListener listener,
      boolean skipWatchingNonExistNode) {
    throw new NotImplementedException("subscribeOneTimeChildChanges is not implemented");
  }

  /**
   * Unsubscribe the listener to further changes. No-op if the listener is not subscribed to the key.
   * @param key Key to identify the entry.
   * @param listener The listener to unsubscribe.
   */
  void unsubscribeDataChange(String key, DataChangeListener listener);

  /**
   * Unsubscribe the listener to further changes. No-op if the listener is not subscribed to the key.
   * @param key Key to identify the entry.
   * @param listener The listener to unsubscribe.
   */
  void unsubscribeDirectChildChange(String key, DirectChildChangeListener listener);

  /**
   * Unsubscribe the listener to further changes. No-op if the listener is not subscribed to the key.
   * @param key Key to identify the entry.
   * @param listener The listener to unsubscribe.
   */
  void unsubscribeChildChanges(String key, ChildChangeListener listener);

  /**
   * Unsubscribe the listener to further changes. No-op if the listener is not subscribed to the key.
   * @param listener The listener to unsubscribe.
   */
  void unsubscribeConnectStateChanges(ConnectStateChangeListener listener);

  /**
   * Block the call until the given key exists or timeout.
   * @param key Key to monitor.
   * @param timeUnit timeout unit
   * @param timeOut timeout value
   * @return
   */
  boolean waitUntilExists(String key, TimeUnit timeUnit, long timeOut);

  /**
   * Serialize the data in type T to a byte array. This function can be used in API that returns or
   * has input value in byte array format.
   * @param data to be serialized.
   * @param path timeout unit
   * @return
   */
   byte[] serialize(T data, String path);

  /**
   * Serialize a byte array to data in type T. This function can be used in API that returns or
   * has input value in byte array format.
   * @param bytes to be deserialized.
   * @param path timeout unit
   * @return
   */
   T deserialize(byte[] bytes, String path);

  // TODO: Secure CRUD APIs
}