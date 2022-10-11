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


public interface MetaClientInterface<T> {
  enum EntryMode {
    //The node will be deleted upon the client's disconnect.
    EPHEMERAL,
    //The node will not be automatically deleted upon client's disconnect.
    PERSISTENT
  }

  /**
   * Interface representing the metadata of an entry. It contains entry type and version number.
   */
  class Stat {
    private int _version;
    private EntryMode _entryMode;

    public EntryMode getEntryType() {return _entryMode;}
    public int getVersion() {return  _version;}
  }

  //synced CRUD API
  void create(final String key, T data, final EntryMode mode);
  void create(final String key, T data, final EntryMode mode, long ttl);
  /**
   * Set data for a given key.
   */
  void set(final String key, T data, int version);
  /**
   * Update existing data of a given key using an updater. This method will issue a read to get
   * current data and apply updater upon the current data.
   * return: the updated value.
   */
  T update(String key, DataUpdater<T> updater);
  Stat exists(final String key);
  T get(String key);
  /**
   * transactional support
   */
  List<OpResult> transactionOP(final Iterable<Op> ops);
  /**
   * For metadata storage that has hierarchical key space (e.g. ZK), the path would be a parent path,
   * For metadata storage that has non-hierarchical key space (e.g. etcd), the path would be a prefix path.
   */
  List<String> getSubEntryKeys(final String path);
  int countSubEntries(final String path);
  boolean delete(String path);


  /* async CRUD API */
  void asyncCreate(final String key, T data, int version, long ttl,
      AsyncCallback.VoidCallback cb);
  void asyncSet(final String key, T data, int version, AsyncCallback.VoidCallback cb);
  void asyncUpdate(final String key, DataUpdater<T> updater, AsyncCallback.VoidCallback cb);
  void asyncGet(final String key, AsyncCallback.DataCallback cb);
  void countSubEntries(final String path, AsyncCallback.DataCallback cb);
  void asyncExist(final String key, AsyncCallback.StatCallback cb);
  void asyncDelete(final String keys, AsyncCallback.VoidCallback cb);
  void asyncTransaction(final String keys, AsyncCallback.TransactionCallback cb);

  // batched API. return result to user when all request finishes.
  boolean[] create(List<String> key, List<T> data, List<EntryMode> mode, List<Long> ttl);
  boolean[] set(List<String> keys, List<T> values, List<Integer> version);
  List<T> update(List<String> keys, List<DataUpdater<T>> updater);
  List<T> get(List<String> keys);
  List<Stat> exists(List<String> keys);
  boolean[] delete(List<String> keys);

}