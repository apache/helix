package com.linkedin.helix.mock.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.mock.storage.DummyProcess.DummyLeaderStandbyStateModelFactory;
import com.linkedin.helix.mock.storage.DummyProcess.DummyOnlineOfflineStateModelFactory;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;

public class MockParticipant implements Stoppable, Runnable
{
  private static Logger LOG = Logger.getLogger(MockParticipant.class);
  private final String _clusterName;
  private final String _instanceName;
  private final String _zkAddr;

  private final CountDownLatch _countDown = new CountDownLatch(1);
  private final HelixManager _manager;
  private final MockMSModelFactory _msModelFacotry;
  private final MockJobIntf _job;

  // mock master-slave state model
  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE" })
  public class MockMSStateModel extends StateModel
  {
    private MockTransition _transition;
    public MockMSStateModel(MockTransition transition)
    {
      _transition = transition;
    }

    public void setTransition(MockTransition transition)
    {
      _transition = transition;
    }

    @Transition(to="SLAVE",from="OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Become SLAVE from OFFLINE");
      if (_transition != null)
      {
        _transition.doTransition(message, context);

      }
    }

    @Transition(to="MASTER",from="SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Become MASTER from SLAVE");
      if (_transition != null)
      {
        _transition.doTransition(message, context);
      }
    }

    @Transition(to="SLAVE",from="MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Become SLAVE from MASTER");
      if (_transition != null)
      {
        _transition.doTransition(message, context);
      }
    }

    @Transition(to="OFFLINE",from="SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Become OFFLINE from SLAVE");
      if (_transition != null)
      {
        _transition.doTransition(message, context);
      }
    }

    @Override
    public void reset()
    {
      LOG.error("Default MockMSStateModel.reset() invoked");
      if (_transition != null)
      {
        _transition.doReset();
      }
    }
  }

  // mock master slave state model factory
  public class MockMSModelFactory
    extends StateModelFactory<MockMSStateModel>
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
      String partition = message.getStateUnitKey();

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
    public void doTransition(Message message, NotificationContext context)
    {
      try
      {
        Thread.sleep(_delay);
      }
      catch (InterruptedException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public MockParticipant(String clusterName, String instanceName, String zkAddr)
    throws Exception
  {
    this(clusterName, instanceName, zkAddr, null, null);
  }

  public MockParticipant(String clusterName, String instanceName, String zkAddr,
                          MockTransition transition)
    throws Exception
  {
    this(clusterName, instanceName, zkAddr, transition, null);
  }

  public MockParticipant(String clusterName, String instanceName, String zkAddr,
      MockTransition transition, MockJobIntf job)
    throws Exception
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _zkAddr = zkAddr;
    _msModelFacotry = new MockMSModelFactory(transition);

    _manager = HelixManagerFactory.getZKHelixManager(_clusterName,
        _instanceName,
        InstanceType.PARTICIPANT,
        _zkAddr);
    _job = job;
  }

  public void setTransition(MockTransition transition)
  {
    _msModelFacotry.setTrasition(transition);
  }

  public HelixManager getManager()
  {
    return _manager;
  }

  @Override
  public void stop()
  {
    _countDown.countDown();
    _manager.disconnect();
  }

  @Override
  public void run()
  {
    try
    {
      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      stateMach.registerStateModelFactory("MasterSlave", _msModelFacotry);

      DummyLeaderStandbyStateModelFactory lsModelFactory = new DummyLeaderStandbyStateModelFactory(10);
      DummyOnlineOfflineStateModelFactory ofModelFactory = new DummyOnlineOfflineStateModelFactory(10);
      stateMach.registerStateModelFactory("LeaderStandby", lsModelFactory);
      stateMach.registerStateModelFactory("OnlineOffline", ofModelFactory);

      if (_job != null)
      {
        _job.doPreConnectJob(_manager);
      }

      _manager.connect();

      if (_job != null)
      {
        _job.doPostConnectJob(_manager);
      }

      _countDown.await();
    }
    catch (InterruptedException e)
    {
      String msg = "participant:" + _instanceName + ", "
                  + Thread.currentThread().getName()
                  + " interrupted";
      LOG.info(msg);
      // System.err.println(msg);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
