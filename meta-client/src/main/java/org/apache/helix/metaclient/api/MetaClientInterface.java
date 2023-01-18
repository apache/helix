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

import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.helix.metaclient.constants.MetaClientInterruptException;
import org.apache.helix.metaclient.constants.MetaClientTimeoutException;


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
    // Client is connected to server
    CONNECTED,

    // Authentication failed.
    AUTH_FAILED,

    // Server has expired this connection.
    EXPIRED,

    // When client failed to connect server.
    INIT_FAILED,

    // When client explicitly call disconnect.
    CLOSED_BY_CLIENT
  }

  /**
   * Interface representing the metadata of an entry. It contains entry type and version number.
   * TODO: we will add session ID to entry stats in the future
   */
  class Stat {
    private int _version;
    private EntryMode _entryMode;

    public EntryMode getEntryType() {
      return _entryMode;
    }

    public int getVersion() {
      return _version;
    }

    public Stat (EntryMode mode, int version) {
      _version = version;
      _entryMode = mode;
    }
  }

  //synced CRUD API

  /**
   * Create an persistent entry with given key and data. The entry will not be created if there is
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

  // TODO: add TTL create and renew API

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
   * @return: the updated value.
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
   * TODO: define exception type when key does not exist
   * @param key key to identify the entry
   * @return Return data of the entry
   */
  T get(final String key);

  /**
   * API for transaction. The list of operation will be executed as an atomic operation.
   * @param ops a list of operations. These operations will all be executed or non of them.
   * @return Return a list of OpResult.
   */
  List<OpResult> transactionOP(final Iterable<Op> ops) throws KeeperException;;

  /**
   * Return a list of children for the given keys.
   * @param key For metadata storage that has hierarchical key space (e.g. ZK), the key would be
   *            a parent key,
   *            For metadata storage that has non-hierarchical key space (e.g. etcd), the key would
   *            be a prefix key.
   * @eturn Return a list of children keys. Return direct child name only for hierarchical key
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
   * @param cb An user defined VoidCallback implementation that will be invoked when async create return.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.VoidCallback
   */
  void asyncCreate(final String key, final T data, final EntryMode mode,
      AsyncCallback.VoidCallback cb);

  /**
   * The asynchronous version of set.
   * @param key key to identify the entry
   * @param data new data of the entry
   * @param version expected version if the entry. -1 matched any version
   * @param cb An user defined VoidCallback implementation that will be invoked when async create return.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.VoidCallback
   */
  void asyncSet(final String key, final T data, final int version, AsyncCallback.VoidCallback cb);

  /**
   * The asynchronous version of update.
   * @param key key to identify the entry
   * @param updater An updater that modifies the entry value.
   * @param cb An user defined VoidCallback implementation that will be invoked when async create return.
   *           It will contain the newly updated data if update succeeded.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.DataCallback
   */
  void asyncUpdate(final String key, DataUpdater<T> updater, AsyncCallback.DataCallback cb);

  /**
   * The asynchronous version of get.
   * @param key key to identify the entry
   * @param cb An user defined VoidCallback implementation that will be invoked when async get return.
   *           It will contain the entry data if get succeeded.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.DataCallback
   */
  void asyncGet(final String key, AsyncCallback.DataCallback cb);

  /**
   * The asynchronous version of get sub entries.
   * @param key key to identify the entry
   * @param cb An user defined VoidCallback implementation that will be invoked when async count child return.
   *           It will contain the list of child keys if succeeded.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.DataCallback
   */
  void asyncCountChildren(final String key, AsyncCallback.DataCallback cb);

  /**
   * The asynchronous version of get sub entries.
   * @param key key to identify the entry
   * @param cb An user defined VoidCallback implementation that will be invoked when async exist return.
   *           It will contain the stats of the entry if succeeded.
   *           @see org.apache.helix.metaclient.api.AsyncCallback.StatCallback
   */
  void asyncExist(final String key, AsyncCallback.StatCallback cb);

  /**
   * The asynchronous version of delete.
   * @param key key to identify the entry
   * @param cb An user defined VoidCallback implementation that will be invoked when async delete
   *           finish and return.  @see org.apache.helix.metaclient.api.AsyncCallback.DataCallback
   */
  void asyncDelete(final String key, AsyncCallback.VoidCallback cb);

  /**
   * The asynchronous version of transaction operations.
   * @param ops A list of operations
   * @param cb An user defined TransactionCallback implementation that will be invoked when
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
   * @return client current connection state with metadata service.
   */
  ConnectState getClientConnectionState();

  // Event notification APIs, user can register multiple listeners on the same key/connection state.
  // All listeners will be automatically removed when client is disconnected.
  // TODO: add auto re-register listener option

  /**
   * Subscribe change of a particular entry. Including entry data change, entry deletion and creation
   * of the given key.
   * @param key Key to identify the entry
   * @param listener An implementation of DataChangeListener
   *                 @see org.apache.helix.metaclient.api.DataChangeListener
   * @param skipWatchingNonExistNode Will not register lister to an non-exist key if set to true.
   *                                 Please set to false if you are expecting ENTRY_CREATED type.
   * @param persistListener The listener will persist when set to true. Otherwise it will be a one
   *                        time triggered listener.
   * @return Return an boolean indication if subscribe succeeded.
   */
  boolean subscribeDataChange(String key, DataChangeListener listener,
       boolean skipWatchingNonExistNode, boolean persistListener);

  /**
   * Subscribe for direct child change event on a particular key. It includes new child
   * creation or deletion. It does not include existing child data change.
   * For hierarchy key spaces like zookeeper, it refers to an entry's direct children nodes.
   * For flat key spaces, it refers to keys that matches `prefix*separator`.
   * @param key key to identify the entry.
   * @param listener An implementation of DirectSubEntryChangeListener.
   *                 @see org.apache.helix.metaclient.api.DirectChildChangeListener
   * @param skipWatchingNonExistNode If the passed in key does not exist, no listener wil be registered.
   * @param persistListener The listener will persist when set to true. Otherwise it will be a one
   *                        time triggered listener.
   *
   * @return Return an DirectSubEntrySubscribeResult. It will contain a list of direct sub children if
   *         subscribe succeeded.
   */
  DirectChildSubscribeResult subscribeDirectChildChange(String key,
      DirectChildChangeListener listener, boolean skipWatchingNonExistNode,
      boolean persistListener);

  /**
   * Subscribe for connection state change.
   * @param listener An implementation of ConnectStateChangeListener.
   *                 @see org.apache.helix.metaclient.api.ConnectStateChangeListener
   * @param persistListener The listener will persist when set to true. Otherwise it will be a one
   *                        time triggered listener.
   *
   * @return Return an boolean indication if subscribe succeeded.
   */
  boolean subscribeStateChanges(ConnectStateChangeListener listener, boolean persistListener);

  /**
   * Subscribe change for all children including entry change and data change.
   * For hierarchy key spaces like zookeeper, it would watch the whole tree structure.
   * For flat key spaces, it would watch for keys with certain prefix.
   * @param key key to identify the entry.
   * @param listener An implementation of ChildChangeListener.
   *                 @see org.apache.helix.metaclient.api.ChildChangeListener
   * @param skipWatchingNonExistNode If the passed in key does not exist, no listener wil be registered.
   * @param persistListener The listener will persist when set to true. Otherwise it will be a one
   *                        time triggered listener.
   */
  boolean subscribeChildChanges(String key, ChildChangeListener listener,
      boolean skipWatchingNonExistNode, boolean persistListener);

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

  // TODO: Secure CRUD APIs
}