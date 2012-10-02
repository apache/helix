/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.mock.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;

import com.linkedin.helix.AccessOption;
import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.mock.storage.DummyProcess.DummyLeaderStandbyStateModelFactory;
import com.linkedin.helix.mock.storage.DummyProcess.DummyOnlineOfflineStateModelFactory;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;
import com.linkedin.helix.store.zk.ZkHelixPropertyStore;

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

  private final HelixManager      _manager;
  private final StateModelFactory _msModelFactory;
  private final MockJobIntf       _job;

  // mock master-slave state model
  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE", "ERROR" })
  public static class MockMSStateModel extends StateModel
  {
    protected MockTransition _transition;

    public MockMSStateModel(MockTransition transition)
    {
      _transition = transition;
    }

    public void setTransition(MockTransition transition)
    {
      _transition = transition;
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become SLAVE from OFFLINE");
      if (_transition != null)
      {
        _transition.doTransition(message, context);

      }
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become MASTER from SLAVE");
      if (_transition != null)
      {
        _transition.doTransition(message, context);
      }
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become SLAVE from MASTER");
      if (_transition != null)
      {
        _transition.doTransition(message, context);
      }
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become OFFLINE from SLAVE");
      if (_transition != null)
      {
        _transition.doTransition(message, context);
      }
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become DROPPED from OFFLINE");
      if (_transition != null)
      {
        _transition.doTransition(message, context);
      }
    }

    @Transition(to = "OFFLINE", from = "ERROR")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) throws InterruptedException
    {
      LOG.info("Become OFFLINE from ERROR");
      // System.err.println("Become OFFLINE from ERROR");
      if (_transition != null)
      {
        _transition.doTransition(message, context);
      }
    }

    @Override
    public void reset()
    {
      LOG.info("Default MockMSStateModel.reset() invoked");
      if (_transition != null)
      {
        _transition.doReset();
      }
    }
  }

  // mock master slave state model factory
  public static class MockMSModelFactory extends StateModelFactory<MockMSStateModel>
  {
    private final MockTransition _transition;

    public MockMSModelFactory()
    {
      this(null);
    }

    public MockMSModelFactory(MockTransition transition)
    {
      _transition = transition;
    }

    public void setTrasition(MockTransition transition)
    {
      Map<String, MockMSStateModel> stateModelMap = getStateModelMap();
      for (MockMSStateModel stateModel : stateModelMap.values())
      {
        stateModel.setTransition(transition);
      }
    }

    @Override
    public MockMSStateModel createNewStateModel(String partitionKey)
    {
      MockMSStateModel model = new MockMSStateModel(_transition);

      return model;
    }
  }

  // mock STORAGE_DEFAULT_SM_SCHEMATA state model
  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "DROPPED", "ERROR" })
  public class MockSchemataStateModel extends StateModel
  {
    @Transition(to = "MASTER", from = "OFFLINE")
    public void onBecomeMasterFromOffline(Message message, NotificationContext context)
    {
      LOG.info("Become MASTER from OFFLINE");
    }

    @Transition(to = "OFFLINE", from = "MASTER")
    public void onBecomeOfflineFromMaster(Message message, NotificationContext context)
    {
      LOG.info("Become OFFLINE from MASTER");
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
    {
      LOG.info("Become DROPPED from OFFLINE");
    }

    @Transition(to = "OFFLINE", from = "ERROR")
    public void onBecomeOfflineFromError(Message message, NotificationContext context)
    {
      LOG.info("Become OFFLINE from ERROR");
    }
  }

  // mock Bootstrap state model
  @StateModelInfo(initialState = "OFFLINE", states = { "ONLINE", "BOOTSTRAP", "OFFLINE",
      "IDLE" })
  public static class MockBootstrapStateModel extends StateModel
  {
    // Overwrite the default value of intial state
    MockBootstrapStateModel()
    {
      _currentState = "IDLE";
    }

    @Transition(to = "OFFLINE", from = "IDLE")
    public void onBecomeOfflineFromIdle(Message message, NotificationContext context)
    {
      LOG.info("Become OFFLINE from IDLE");
    }

    @Transition(to = "BOOTSTRAP", from = "OFFLINE")
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context)
    {
      LOG.info("Become BOOTSTRAP from OFFLINE");
    }

    @Transition(to = "ONLINE", from = "BOOSTRAP")
    public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context)
    {
      LOG.info("Become ONLINE from BOOTSTRAP");
    }

    @Transition(to = "OFFLINE", from = "ONLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context)
    {
      LOG.info("Become OFFLINE from ONLINE");
    }
  }

  // mock STORAGE_DEFAULT_SM_SCHEMATA state model factory
  public class MockSchemataModelFactory extends StateModelFactory<MockSchemataStateModel>
  {
    @Override
    public MockSchemataStateModel createNewStateModel(String partitionKey)
    {
      MockSchemataStateModel model = new MockSchemataStateModel();
      return model;
    }
  }

  // mock Bootstrap state model factory
  public static class MockBootstrapModelFactory extends
      StateModelFactory<MockBootstrapStateModel>
  {
    @Override
    public MockBootstrapStateModel createNewStateModel(String partitionKey)
    {
      MockBootstrapStateModel model = new MockBootstrapStateModel();
      return model;
    }
  }

  // simulate error transition
  public static class ErrTransition extends MockTransition
  {
    private final Map<String, Set<String>> _errPartitions;

    public ErrTransition(Map<String, Set<String>> errPartitions)
    {
      if (errPartitions != null)
      {
        // change key to upper case
        _errPartitions = new HashMap<String, Set<String>>();
        for (String key : errPartitions.keySet())
        {
          String upperKey = key.toUpperCase();
          _errPartitions.put(upperKey, errPartitions.get(key));
        }
      }
      else
      {
        _errPartitions = Collections.emptyMap();
      }
    }

    @Override
    public void doTransition(Message message, NotificationContext context)
    {
      String fromState = message.getFromState();
      String toState = message.getToState();
      String partition = message.getPartitionName();

      String key = (fromState + "-" + toState).toUpperCase();
      if (_errPartitions.containsKey(key) && _errPartitions.get(key).contains(partition))
      {
        String errMsg =
            "IGNORABLE: test throw exception for " + partition + " transit from "
                + fromState + " to " + toState;
        throw new RuntimeException(errMsg);
      }
    }
  }

  // simulate long transition
  public static class SleepTransition extends MockTransition
  {
    private final long _delay;

    public SleepTransition(long delay)
    {
      _delay = delay > 0 ? delay : 0;
    }

    @Override
    public void doTransition(Message message, NotificationContext context) throws InterruptedException
    {
      Thread.sleep(_delay);

    }
  }

  // simulate access property store and update one znode
  public static class StoreAccessOneNodeTransition extends MockTransition
  {
    @Override
    public void doTransition(Message message, NotificationContext context) throws InterruptedException
    {
      HelixManager manager = context.getManager();
      ZkHelixPropertyStore<ZNRecord> store = manager.getHelixPropertyStore();
      final String setPath = "/TEST_PERF/set";
      final String updatePath = "/TEST_PERF/update";
      final String key = message.getPartitionName();
      try
      {
        // get/set once
        ZNRecord record = null;
        try
        {
          record = store.get(setPath, null, 0);
        }
        catch (ZkNoNodeException e)
        {
          record = new ZNRecord(setPath);
        }
        record.setSimpleField("setTimestamp", "" + System.currentTimeMillis());
        store.set(setPath, record, AccessOption.PERSISTENT);

        // update once
        store.update(updatePath, new DataUpdater<ZNRecord>()
        {

          @Override
          public ZNRecord update(ZNRecord currentData)
          {
            if (currentData == null)
            {
              currentData = new ZNRecord(updatePath);
            }
            currentData.setSimpleField(key, "" + System.currentTimeMillis());

            return currentData;
          }

        }, AccessOption.PERSISTENT);
      }
      catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }
  }

  // simulate access property store and update different znodes
  public static class StoreAccessDiffNodeTransition extends MockTransition
  {
    @Override
    public void doTransition(Message message, NotificationContext context) throws InterruptedException
    {
      HelixManager manager = context.getManager();
      ZkHelixPropertyStore<ZNRecord> store = manager.getHelixPropertyStore();
      final String setPath = "/TEST_PERF/set/" + message.getPartitionName();
      final String updatePath = "/TEST_PERF/update/" + message.getPartitionName();
      // final String key = message.getPartitionName();
      try
      {
        // get/set once
        ZNRecord record = null;
        try
        {
          record = store.get(setPath, null, 0);
        }
        catch (ZkNoNodeException e)
        {
          record = new ZNRecord(setPath);
        }
        record.setSimpleField("setTimestamp", "" + System.currentTimeMillis());
        store.set(setPath, record, AccessOption.PERSISTENT);

        // update once
        store.update(updatePath, new DataUpdater<ZNRecord>()
        {

          @Override
          public ZNRecord update(ZNRecord currentData)
          {
            if (currentData == null)
            {
              currentData = new ZNRecord(updatePath);
            }
            currentData.setSimpleField("updateTimestamp", "" + System.currentTimeMillis());

            return currentData;
          }

        }, AccessOption.PERSISTENT);
      }
      catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }
  }
  
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

    _manager =
        HelixManagerFactory.getZKHelixManager(_clusterName,
                                              _instanceName,
                                              InstanceType.PARTICIPANT,
                                              zkAddr);
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

    _manager =
        HelixManagerFactory.getZKHelixManager(_clusterName,
                                              _instanceName,
                                              InstanceType.PARTICIPANT,
                                              zkAddr);
    _job = job;
  }

  public StateModelFactory getStateModelFactory()
  {
    return _msModelFactory;
  }

  public MockParticipant(HelixManager manager, MockTransition transition)
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

  public HelixManager getManager()
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
