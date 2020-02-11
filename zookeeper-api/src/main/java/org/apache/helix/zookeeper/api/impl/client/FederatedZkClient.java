package org.apache.helix.zookeeper.api.impl.client;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.helix.zookeeper.api.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.api.zkclient.DataUpdater;
import org.apache.helix.zookeeper.api.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.api.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.api.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.api.zkclient.deprecated.IZkStateListener;
import org.apache.helix.zookeeper.api.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.api.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


public class FederatedZkClient implements RealmAwareZkClient {
  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    return null;
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener) {

  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {

  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener) {

  }

  @Override
  public void subscribeStateChanges(IZkStateListener listener) {

  }

  @Override
  public void unsubscribeStateChanges(IZkStateListener listener) {

  }

  @Override
  public void unsubscribeAll() {

  }

  @Override
  public void createPersistent(String path) {

  }

  @Override
  public void createPersistent(String path, boolean createParents) {

  }

  @Override
  public void createPersistent(String path, boolean createParents, List<ACL> acl) {

  }

  @Override
  public void createPersistent(String path, Object data) {

  }

  @Override
  public void createPersistent(String path, Object data, List<ACL> acl) {

  }

  @Override
  public String createPersistentSequential(String path, Object data) {
    return null;
  }

  @Override
  public String createPersistentSequential(String path, Object data, List<ACL> acl) {
    return null;
  }

  @Override
  public void createEphemeral(String path) {

  }

  @Override
  public void createEphemeral(String path, String sessionId) {

  }

  @Override
  public void createEphemeral(String path, List<ACL> acl) {

  }

  @Override
  public void createEphemeral(String path, List<ACL> acl, String sessionId) {

  }

  @Override
  public String create(String path, Object data, CreateMode mode) {
    return null;
  }

  @Override
  public String create(String path, Object datat, List<ACL> acl, CreateMode mode) {
    return null;
  }

  @Override
  public void createEphemeral(String path, Object data) {

  }

  @Override
  public void createEphemeral(String path, Object data, String sessionId) {

  }

  @Override
  public void createEphemeral(String path, Object data, List<ACL> acl) {

  }

  @Override
  public void createEphemeral(String path, Object data, List<ACL> acl, String sessionId) {

  }

  @Override
  public String createEphemeralSequential(String path, Object data) {
    return null;
  }

  @Override
  public String createEphemeralSequential(String path, Object data, List<ACL> acl) {
    return null;
  }

  @Override
  public String createEphemeralSequential(String path, Object data, String sessionId) {
    return null;
  }

  @Override
  public String createEphemeralSequential(String path, Object data, List<ACL> acl,
      String sessionId) {
    return null;
  }

  @Override
  public List<String> getChildren(String path) {
    return null;
  }

  @Override
  public int countChildren(String path) {
    return 0;
  }

  @Override
  public boolean exists(String path) {
    return false;
  }

  @Override
  public Stat getStat(String path) {
    return null;
  }

  @Override
  public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) {
    return false;
  }

  @Override
  public void deleteRecursively(String path) {

  }

  @Override
  public boolean delete(String path) {
    return false;
  }

  @Override
  public <T> T readData(String path) {
    return null;
  }

  @Override
  public <T> T readData(String path, boolean returnNullIfPathNotExists) {
    return null;
  }

  @Override
  public <T> T readData(String path, Stat stat) {
    return null;
  }

  @Override
  public <T> T readData(String path, Stat stat, boolean watch) {
    return null;
  }

  @Override
  public <T> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists) {
    return null;
  }

  @Override
  public void writeData(String path, Object object) {

  }

  @Override
  public <T> void updateDataSerialized(String path, DataUpdater<T> updater) {

  }

  @Override
  public void writeData(String path, Object datat, int expectedVersion) {

  }

  @Override
  public Stat writeDataReturnStat(String path, Object datat, int expectedVersion) {
    return null;
  }

  @Override
  public Stat writeDataGetStat(String path, Object datat, int expectedVersion) {
    return null;
  }

  @Override
  public void asyncCreate(String path, Object datat, CreateMode mode,
      ZkAsyncCallbacks.CreateCallbackHandler cb) {

  }

  @Override
  public void asyncSetData(String path, Object datat, int version,
      ZkAsyncCallbacks.SetDataCallbackHandler cb) {

  }

  @Override
  public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {

  }

  @Override
  public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {

  }

  @Override
  public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {

  }

  @Override
  public void watchForData(String path) {

  }

  @Override
  public List<String> watchForChilds(String path) {
    return null;
  }

  @Override
  public long getCreationTime(String path) {
    return 0;
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) {
    return null;
  }

  @Override
  public boolean waitUntilConnected(long time, TimeUnit timeUnit) {
    return false;
  }

  @Override
  public String getServers() {
    return null;
  }

  @Override
  public long getSessionId() {
    return 0;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public byte[] serialize(Object data, String path) {
    return new byte[0];
  }

  @Override
  public <T> T deserialize(byte[] data, String path) {
    return null;
  }

  @Override
  public void setZkSerializer(ZkSerializer zkSerializer) {

  }

  @Override
  public void setZkSerializer(PathBasedZkSerializer zkSerializer) {

  }

  @Override
  public PathBasedZkSerializer getZkSerializer() {
    return null;
  }
}
