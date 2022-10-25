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


import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.helix.metaclient.api.AsyncCallback;
import org.apache.helix.metaclient.api.ConnectStateChangeListener;
import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.metaclient.api.DataUpdater;
import org.apache.helix.metaclient.api.DirectChildrenChangeListener;
import org.apache.helix.metaclient.api.DirectEntrySubscribeResult;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.api.OpResult;
import org.apache.helix.metaclient.api.PersistSubEntryChangeListener;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;


public class ZkMetaClient implements MetaClientInterface {

  private RealmAwareZkClient _zkClient;

  public ZkMetaClient() {

  }

  @Override
  public void create(String key, Object data, EntryMode mode) {

  }

  @Override
  public void create(String key, Object data, EntryMode mode, long ttl) {

  }

  @Override
  public void set(String key, Object data, int version) {

  }

  @Override
  public Object update(String key, DataUpdater updater) {
    return null;
  }

  @Override
  public Stat exists(String key) {
    return null;
  }

  @Override
  public Object get(String key) {
    return null;
  }

  @Override
  public List<String> getSubEntryKeys(String path) {
    return null;
  }

  @Override
  public int countSubEntries(String path) {
    return 0;
  }

  @Override
  public boolean delete(String path) {
    return false;
  }

  @Override
  public boolean recursiveDelete(String path) {
    return false;
  }

  @Override
  public void asyncCreate(String key, Object data, int version, long ttl,
      AsyncCallback.VoidCallback cb) {

  }

  @Override
  public void asyncSet(String key, Object data, int version, AsyncCallback.VoidCallback cb) {

  }

  @Override
  public void asyncUpdate(String key, DataUpdater updater, AsyncCallback.VoidCallback cb) {

  }

  @Override
  public void asyncGet(String key, AsyncCallback.DataCallback cb) {

  }

  @Override
  public void asyncCountSubEntries(String path, AsyncCallback.DataCallback cb) {

  }

  @Override
  public void asyncExist(String key, AsyncCallback.StatCallback cb) {

  }

  @Override
  public void asyncDelete(String keys, AsyncCallback.VoidCallback cb) {

  }

  @Override
  public void asyncTransaction(String keys, AsyncCallback.TransactionCallback cb) {

  }

  @Override
  public ConnectState connect() {
    return null;
  }

  @Override
  public void disconnect() {

  }

  @Override
  public boolean subscribeDataChange(String key, DataChangeListener listener,
      DataChangeListener.ChangeType eventType, boolean skipWatchingNonExistNode,
      boolean persistListener) {
    return false;
  }

  @Override
  public DirectEntrySubscribeResult subscribeDirectEntryChange(String key,
      DirectChildrenChangeListener listener, boolean skipWatchingNonExistNode,
      boolean persistListener) {
    return null;
  }

  @Override
  public boolean subscribeStateChanges(ConnectStateChangeListener listener) {
    return false;
  }

  @Override
  public boolean subscribeEntryChanges(String key, PersistSubEntryChangeListener listener) {
    return false;
  }

  @Override
  public void unsubscribeDataChange(String key, DataChangeListener listener,
      DataChangeListener.ChangeType eventType) {

  }

  @Override
  public void unsubscribeDirectEntryChange(String key, DirectChildrenChangeListener listener) {

  }

  @Override
  public void unsubscribeEntryChanges(String key, PersistSubEntryChangeListener listener) {

  }

  @Override
  public void unsubscribeConnectStateChanges(ConnectStateChangeListener listener) {

  }

  @Override
  public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) {
    return false;
  }

  @Override
  public boolean[] delete(List keys) {
    return new boolean[0];
  }

  @Override
  public List<Stat> exists(List keys) {
    return null;
  }

  @Override
  public List get(List keys) {
    return null;
  }

  @Override
  public List update(List keys, List updater) {
    return null;
  }

  @Override
  public boolean[] set(List keys, List values, List version) {
    return new boolean[0];
  }

  @Override
  public boolean[] create(List key, List data, List mode, List ttl) {
    return new boolean[0];
  }

  @Override
  public List<OpResult> transactionOP(Iterable iterable) {
    return null;
  }
}
