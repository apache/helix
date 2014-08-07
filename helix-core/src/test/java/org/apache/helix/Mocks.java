package org.apache.helix;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.messaging.handling.MessageTask;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.zookeeper.data.Stat;

public class Mocks {
  public static class MockBaseDataAccessor implements BaseDataAccessor<ZNRecord> {
    Map<String, ZNRecord> map = new HashMap<String, ZNRecord>();

    @Override
    public boolean create(String path, ZNRecord record, int options) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean set(String path, ZNRecord record, int options) {
      System.err.println("Store.write()" + System.currentTimeMillis());
      map.put(path, record);
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return true;
    }

    @Override
    public boolean update(String path, DataUpdater<ZNRecord> updater, int options) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean remove(String path, int options) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean[] createChildren(List<String> paths, List<ZNRecord> records, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean[] setChildren(List<String> paths, List<ZNRecord> records, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean[] updateChildren(List<String> paths, List<DataUpdater<ZNRecord>> updaters,
        int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean[] remove(List<String> paths, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ZNRecord get(String path, Stat stat, int options) {
      return map.get(path);
    }

    @Override
    public List<ZNRecord> get(List<String> paths, List<Stat> stats, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<String> getChildNames(String parentPath, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean exists(String path, int options) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean[] exists(List<String> paths, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Stat[] getStats(List<String> paths, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Stat getStat(String path, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void subscribeDataChanges(String path, IZkDataListener listener) {
      // TODO Auto-generated method stub

    }

    @Override
    public void unsubscribeDataChanges(String path, IZkDataListener listener) {
      // TODO Auto-generated method stub

    }

    @Override
    public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void unsubscribeChildChanges(String path, IZkChildListener listener) {
      // TODO Auto-generated method stub

    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean set(String path, ZNRecord record, int options, int expectVersion) {
      // TODO Auto-generated method stub
      return false;
    }

  }

  public static class MockStateModel extends TransitionHandler {
    boolean stateModelInvoked = false;

    public void onBecomeMasterFromSlave(Message msg, NotificationContext context) {
      stateModelInvoked = true;
    }

    public void onBecomeSlaveFromOffline(Message msg, NotificationContext context) {
      stateModelInvoked = true;
    }
  }

  @StateModelInfo(states = "{'OFFLINE','SLAVE','MASTER'}", initialState = "OFFINE")
  public static class MockStateModelAnnotated extends TransitionHandler {
    boolean stateModelInvoked = false;

    @Transition(from = "SLAVE", to = "MASTER")
    public void slaveToMaster(Message msg, NotificationContext context) {
      stateModelInvoked = true;
    }

    @Transition(from = "OFFLINE", to = "SLAVE")
    public void offlineToSlave(Message msg, NotificationContext context) {
      stateModelInvoked = true;
    }
  }

  public static class MockHelixTaskExecutor extends HelixTaskExecutor {
    boolean completionInvoked = false;

    @Override
    public void finishTask(MessageTask task) {
      System.out.println("Mocks.MockCMTaskExecutor.finishTask()");
      completionInvoked = true;
    }

    public boolean isDone(String taskId) {
      Future<HelixTaskResult> future = _taskMap.get(taskId).getFuture();
      if (future != null) {
        return future.isDone();
      }
      return false;
    }
  }

  public static class MockManager implements HelixManager {
    MockAccessor accessor;

    private final String _clusterName;
    private final String _sessionId;
    String _instanceName;
    ClusterMessagingService _msgSvc;
    private String _version;

    HelixManagerProperties _properties = new HelixManagerProperties();

    public MockManager() {
      this("testCluster-" + Math.random() * 10000 % 999);
    }

    public MockManager(String clusterName) {
      _clusterName = clusterName;
      accessor = new MockAccessor(clusterName);
      _sessionId = UUID.randomUUID().toString();
      _instanceName = "testInstanceName";
      _msgSvc = new MockClusterMessagingService();
    }

    @Override
    public void disconnect() {

    }

    @Override
    public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
      // TODO Auto-generated method stub

    }

    @Override
    public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) {
      // TODO Auto-generated method stub

    }

    @Override
    public void addMessageListener(MessageListener listener, String instanceName) {
      // TODO Auto-generated method stub

    }

    @Override
    public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
        String instanceName, String sessionId) {
      // TODO Auto-generated method stub

    }

    @Override
    public void addExternalViewChangeListener(ExternalViewChangeListener listener) {
      // TODO Auto-generated method stub

    }

    @Override
    public String getClusterName() {
      return _clusterName;
    }

    @Override
    public String getInstanceName() {
      return _instanceName;
    }

    @Override
    public void connect() {
      // TODO Auto-generated method stub

    }

    @Override
    public String getSessionId() {
      return _sessionId;
    }

    @Override
    public boolean isConnected() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public long getLastNotificationTime() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void addControllerListener(ControllerChangeListener listener) {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean removeListener(PropertyKey key, Object listener) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public HelixAdmin getClusterManagmentTool() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ClusterMessagingService getMessagingService() {
      // TODO Auto-generated method stub
      return _msgSvc;
    }

    @Override
    public InstanceType getInstanceType() {
      return InstanceType.PARTICIPANT;
    }

    @Override
    public String getVersion() {
      return _version;
    }

    public void setVersion(String version) {
      _properties.getProperties().put("clustermanager.version", version);
      _version = version;

    }

    @Override
    public StateMachineEngine getStateMachineEngine() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean isLeader() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public ConfigAccessor getConfigAccessor() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void startTimerTasks() {
      // TODO Auto-generated method stub

    }

    @Override
    public void stopTimerTasks() {
      // TODO Auto-generated method stub
    }

    @Override
    public HelixDataAccessor getHelixDataAccessor() {
      return accessor;
    }

    @Override
    public void addPreConnectCallback(PreConnectCallback callback) {
      // TODO Auto-generated method stub
    }

    @Override
    public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener)
        throws Exception {
      // TODO Auto-generated method stub
    }

    @Override
    public void addConfigChangeListener(ScopedConfigChangeListener listener,
        ConfigScopeProperty scope) throws Exception {
      // TODO Auto-generated method stub
    }

    @Override
    public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
      // TODO Auto-generated method stub
    }

    @Override
    public HelixManagerProperties getProperties() {
      // TODO Auto-generated method stub
      return _properties;
    }

    @Override
    public void addControllerMessageListener(MessageListener listener) {
      // TODO Auto-generated method stub
    }

  }

  public static class MockAccessor implements HelixDataAccessor {
    private final String _clusterName;
    Map<String, ZNRecord> data = new HashMap<String, ZNRecord>();
    private final Builder _propertyKeyBuilder;

    public MockAccessor() {
      this("testCluster-" + Math.random() * 10000 % 999);
    }

    public MockAccessor(String clusterName) {
      _clusterName = clusterName;
      _propertyKeyBuilder = new PropertyKey.Builder(_clusterName);
    }

    Map<String, ZNRecord> map = new HashMap<String, ZNRecord>();

    @Override
    public boolean setProperty(PropertyKey key, HelixProperty value) {
      String path = key.getPath();
      data.put(path, value.getRecord());
      return true;
    }

    @Override
    public <T extends HelixProperty> boolean updateProperty(PropertyKey key, T value) {
      String path = key.getPath();
      PropertyType type = key.getType();
      if (type.updateOnlyOnExists) {
        if (data.containsKey(path)) {
          if (type.mergeOnUpdate) {
            ZNRecord znRecord = new ZNRecord(data.get(path));
            znRecord.merge(value.getRecord());
            data.put(path, znRecord);
          } else {
            data.put(path, value.getRecord());
          }
        }
      } else {
        if (type.mergeOnUpdate) {
          if (data.containsKey(path)) {
            ZNRecord znRecord = new ZNRecord(data.get(path));
            znRecord.merge(value.getRecord());
            data.put(path, znRecord);
          } else {
            data.put(path, value.getRecord());
          }
        } else {
          data.put(path, value.getRecord());
        }
      }

      return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends HelixProperty> T getProperty(PropertyKey key) {
      String path = key.getPath();
      return (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), data.get(path));
    }

    @Override
    public boolean removeProperty(PropertyKey key) {
      String path = key.getPath(); // PropertyPathConfig.getPath(type,
      // _clusterName, keys);
      data.remove(path);
      return true;
    }

    @Override
    public List<String> getChildNames(PropertyKey propertyKey) {
      List<String> child = new ArrayList<String>();
      String path = propertyKey.getPath();
      for (String key : data.keySet()) {
        if (key.startsWith(path)) {
          String[] keySplit = key.split("\\/");
          String[] pathSplit = path.split("\\/");
          if (keySplit.length > pathSplit.length) {
            child.add(keySplit[pathSplit.length]);
          }
        }
      }
      return child;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends HelixProperty> List<T> getChildValues(PropertyKey propertyKey) {
      List<ZNRecord> childs = new ArrayList<ZNRecord>();
      String path = propertyKey.getPath(); // PropertyPathConfig.getPath(type,
      // _clusterName, keys);
      for (String key : data.keySet()) {
        if (key.startsWith(path)) {
          String[] keySplit = key.split("\\/");
          String[] pathSplit = path.split("\\/");
          if (keySplit.length - pathSplit.length == 1) {
            ZNRecord record = data.get(key);
            if (record != null) {
              childs.add(record);
            }
          } else {
            System.out.println("keySplit:" + Arrays.toString(keySplit));
            System.out.println("pathSplit:" + Arrays.toString(pathSplit));
          }
        }
      }
      return (List<T>) HelixProperty.convertToTypedList(propertyKey.getTypeClass(), childs);
    }

    @Override
    public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key) {
      List<T> list = getChildValues(key);
      return HelixProperty.convertListToMap(list);
    }

    @Override
    public <T extends HelixProperty> boolean createProperty(PropertyKey key, T value) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public <T extends HelixProperty> boolean[] createChildren(List<PropertyKey> keys,
        List<T> children) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public <T extends HelixProperty> boolean[] setChildren(List<PropertyKey> keys, List<T> children) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Builder keyBuilder() {
      return _propertyKeyBuilder;
    }

    @Override
    public BaseDataAccessor<ZNRecord> getBaseDataAccessor() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public <T extends HelixProperty> boolean[] updateChildren(List<String> paths,
        List<DataUpdater<ZNRecord>> updaters, int options) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys) {
      List<T> list = new ArrayList<T>();
      for (PropertyKey key : keys) {
        @SuppressWarnings("unchecked")
        T t = (T) getProperty(key);
        list.add(t);
      }
      return list;
    }
  }

  public static class MockClusterMessagingService implements ClusterMessagingService {

    @Override
    public int send(Criteria recipientCriteria, Message message) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int send(Criteria receipientCriteria, Message message, AsyncCallback callbackOnReply,
        int timeOut) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int sendAndWait(Criteria receipientCriteria, Message message,
        AsyncCallback callbackOnReply, int timeOut) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory) {
      // TODO Auto-generated method stub

    }

    @Override
    public int send(Criteria receipientCriteria, Message message, AsyncCallback callbackOnReply,
        int timeOut, int retryCount) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int sendAndWait(Criteria receipientCriteria, Message message,
        AsyncCallback callbackOnReply, int timeOut, int retryCount) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public Map<InstanceType, List<Message>> generateMessage(Criteria recipientCriteria,
        Message messageTemplate) {
      // TODO Auto-generated method stub
      return null;
    }

  }
}
