package org.apache.helix.mock.participant;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkHelixTestManager;
import org.apache.helix.mock.participant.DummyProcess.DummyLeaderStandbyStateModelFactory;
import org.apache.helix.mock.participant.DummyProcess.DummyOnlineOfflineStateModelFactory;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;


public class MockParticipant extends Thread
{
  private static Logger           LOG                      =
                                                               Logger.getLogger(MockParticipant.class);
  private final String            _clusterName;
  private final String            _instanceName;
  // private final String _zkAddr;

  private final CountDownLatch    _startCountDown          = new CountDownLatch(1);
  private final CountDownLatch    _stopCountDown           = new CountDownLatch(1);
  private final CountDownLatch    _waitStopFinishCountDown = new CountDownLatch(1);

  private final ZkHelixTestManager _manager;
  private final StateModelFactory _msModelFactory;
  private final MockJobIntf       _job;

  public MockParticipant(String clusterName, String instanceName, String zkAddr) throws Exception
  {
    this(clusterName, instanceName, zkAddr, null, null);
  }

  public MockParticipant(String clusterName,
                         String instanceName,
                         String zkAddr,
                         MockTransition transition) throws Exception
  {
    this(clusterName, instanceName, zkAddr, transition, null);
  }

  public MockParticipant(String clusterName,
                         String instanceName,
                         String zkAddr,
                         MockTransition transition,
                         MockJobIntf job) throws Exception
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _msModelFactory = new MockMSModelFactory(transition);

    _manager = new ZkHelixTestManager(_clusterName, _instanceName, InstanceType.PARTICIPANT, zkAddr);
    _job = job;
  }

  public MockParticipant(StateModelFactory factory,
                         String clusterName,
                         String instanceName,
                         String zkAddr,
                         MockJobIntf job) throws Exception
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _msModelFactory = factory;

    _manager = new ZkHelixTestManager(_clusterName, _instanceName, InstanceType.PARTICIPANT, zkAddr);
    _job = job;
  }

  public StateModelFactory getStateModelFactory()
  {
    return _msModelFactory;
  }

  public MockParticipant(ZkHelixTestManager manager, MockTransition transition)
  {
    _clusterName = manager.getClusterName();
    _instanceName = manager.getInstanceName();
    _manager = manager;

    _msModelFactory = new MockMSModelFactory(transition);
    _job = null;
  }

  public void setTransition(MockTransition transition)
  {
    if (_msModelFactory instanceof MockMSModelFactory)
    {
      ((MockMSModelFactory) _msModelFactory).setTrasition(transition);
    }
  }

  public ZkHelixTestManager getManager()
  {
    return _manager;
  }

  public String getInstanceName()
  {
    return _instanceName;
  }

  public String getClusterName()
  {
    return _clusterName;
  }

  public void syncStop()
  {
    _stopCountDown.countDown();
    try
    {
      _waitStopFinishCountDown.await();
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // synchronized (_manager)
    // {
    // _manager.disconnect();
    // }
  }

  public void syncStart()
  {
    super.start();
    try
    {
      _startCountDown.await();
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void run()
  {
    try
    {
      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      stateMach.registerStateModelFactory("MasterSlave", _msModelFactory);

      DummyLeaderStandbyStateModelFactory lsModelFactory =
          new DummyLeaderStandbyStateModelFactory(10);
      DummyOnlineOfflineStateModelFactory ofModelFactory =
          new DummyOnlineOfflineStateModelFactory(10);
      stateMach.registerStateModelFactory("LeaderStandby", lsModelFactory);
      stateMach.registerStateModelFactory("OnlineOffline", ofModelFactory);

      MockSchemataModelFactory schemataFactory = new MockSchemataModelFactory();
      stateMach.registerStateModelFactory("STORAGE_DEFAULT_SM_SCHEMATA", schemataFactory);
      // MockBootstrapModelFactory bootstrapFactory = new MockBootstrapModelFactory();
      // stateMach.registerStateModelFactory("Bootstrap", bootstrapFactory);

      if (_job != null)
      {
        _job.doPreConnectJob(_manager);
      }

      _manager.connect();
      _startCountDown.countDown();

      if (_job != null)
      {
        _job.doPostConnectJob(_manager);
      }

      _stopCountDown.await();
    }
    catch (InterruptedException e)
    {
      String msg =
          "participant: " + _instanceName + ", " + Thread.currentThread().getName()
              + " is interrupted";
      LOG.info(msg);
      System.err.println(msg);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    finally
    {
      _startCountDown.countDown();

      synchronized (_manager)
      {
        _manager.disconnect();
      }
      _waitStopFinishCountDown.countDown();
    }
  }
}
