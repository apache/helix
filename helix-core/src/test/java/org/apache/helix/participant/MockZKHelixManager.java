package org.apache.helix.participant;

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

import java.util.UUID;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigChangeListener;
import org.apache.helix.model.ConfigScope.ConfigScopeProperty;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HealthStateChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.MessageListener;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.ScopedConfigChangeListener;
import org.apache.helix.ZNRecord;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public class MockZKHelixManager implements HelixManager
{
  private final ZKHelixDataAccessor _accessor;
  private final String              _instanceName;
  private final String              _clusterName;
  private final InstanceType        _type;

  public MockZKHelixManager(String clusterName,
                            String instanceName,
                            InstanceType type,
                            ZkClient zkClient)
  {
    _instanceName = instanceName;
    _clusterName = clusterName;
    _type = type;
    _accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));
  }

  @Override
  public void connect() throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isConnected()
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void disconnect()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener) throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName) throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
                                            String instanceName,
                                            String sessionId) throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean removeListener(PropertyKey key, Object listener)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public HelixDataAccessor getHelixDataAccessor()
  {
    return _accessor;
  }

  @Override
  public String getClusterName()
  {
    return _clusterName;
  }

  @Override
  public String getInstanceName()
  {
    return _instanceName;
  }

  @Override
  public String getSessionId()
  {
    // TODO Auto-generated method stub
    return UUID.randomUUID().toString();
  }

  @Override
  public long getLastNotificationTime()
  {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public HelixAdmin getClusterManagmentTool()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClusterMessagingService getMessagingService()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InstanceType getInstanceType()
  {
    return _type;
  }

  @Override
  public void addHealthStateChangeListener(HealthStateChangeListener listener,
                                           String instanceName) throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public String getVersion()
  {
    // TODO Auto-generated method stub
    return UUID.randomUUID().toString();
  }

  @Override
  public StateMachineEngine getStateMachineEngine()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isLeader()
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ConfigAccessor getConfigAccessor()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void startTimerTasks()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void stopTimerTasks()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore()
  {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener) throws Exception {
  	// TODO Auto-generated method stub

  }

  @Override
  public void addConfigChangeListener(ScopedConfigChangeListener listener, ConfigScopeProperty scope)
          throws Exception {
  	// TODO Auto-generated method stub

  }

  @Override
  public void setLiveInstanceInfoProvider(
      LiveInstanceInfoProvider liveInstanceInfoProvider)
  {
    // TODO Auto-generated method stub
    
  }

}
