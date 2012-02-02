package com.linkedin.helix.mock.storage;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.ClusterManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.mock.storage.DummyProcess.DummyLeaderStandbyStateModelFactory;
import com.linkedin.helix.mock.storage.DummyProcess.DummyOnlineOfflineStateModelFactory;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.StateMachEngine;
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
  private final ClusterManager _manager;
  private final MockMSModelFactory _msModelFacotry;
  private final MockJobIntf _job;

  // mock master-slave state model
  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE" })
  public class MockMSStateModel extends StateModel
  {
    private MockTransitionIntf _transition;
    public MockMSStateModel(MockTransitionIntf transition)
    {
      _transition = transition;
    }

    public void resetTransition()
    {
      _transition = null;
    }
    
    @Transition(to="SLAVE",from="OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Become SLAVE from OFFLINE");
      if (_transition != null)
      {
        _transition.doTrasition(message, context);
        
      }
    }

    @Transition(to="MASTER",from="SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Become MASTER from SLAVE");
      if (_transition != null)
      {
        _transition.doTrasition(message, context);
      }
    }

    @Transition(to="SLAVE",from="MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Become SLAVE from MASTER");
      if (_transition != null)
      {
        _transition.doTrasition(message, context);
      }
    }

    @Transition(to="OFFLINE",from="SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Become OFFLINE from SLAVE");
      if (_transition != null)
      {
        _transition.doTrasition(message, context);
      }
    }
  }

  // mock master slave state model factory
  public class MockMSModelFactory
    extends StateModelFactory<MockMSStateModel>
  {
    private MockTransitionIntf _transition;
    
    public MockMSModelFactory()
    {
      this(null);
    }
    
    public MockMSModelFactory(MockTransitionIntf transition)
    {
      _transition = transition;
    }
    
    public void resetTrasition()
    {
      _transition = null;
      Map<String, MockMSStateModel> stateModelMap = getStateModelMap();
      for (MockMSStateModel stateModel : stateModelMap.values())
      {
        stateModel.resetTransition();
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
  public static class ErrTransition implements MockTransitionIntf
  {
    private final String _fromState;
    private final String _toState;
    private final Set<String> _errPartitions;
    
    public ErrTransition(String fromState, String toState, Set<String> errPartitions)
    {
      _fromState = fromState;
      _toState = toState;
      _errPartitions = errPartitions;
    }
    
    @Override
    public void doTrasition(Message message, NotificationContext context)
    {
      String fromState = message.getFromState();
      String toState = message.getToState();
      String partition = message.getStateUnitKey();
      
      if (_errPartitions.contains(partition)
          && fromState.equalsIgnoreCase(_fromState)
          && toState.equalsIgnoreCase(_toState))
      {
        String errMsg = "IGNORABLE: test throw exception for " 
                      + partition + " transit from "
                      + fromState + " to " + toState;
        throw new RuntimeException(errMsg);
      }
    } 
  }
  
  // simulate long transition
  public static class SleepTransition implements MockTransitionIntf
  {
    private final long _delay;
    
    public SleepTransition(long delay)
    {
      _delay = delay > 0 ? delay : 0;
    }
    
    @Override
    public void doTrasition(Message message, NotificationContext context)
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
                          MockTransitionIntf transition) 
    throws Exception
  {
    this(clusterName, instanceName, zkAddr, transition, null);
  }
  
  public MockParticipant(String clusterName, String instanceName, String zkAddr, 
      MockTransitionIntf transition, MockJobIntf job) 
    throws Exception
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _zkAddr = zkAddr;
    _msModelFacotry = new MockMSModelFactory(transition);
    
    _manager = ClusterManagerFactory.getZKClusterManager(_clusterName,
        _instanceName,
        InstanceType.PARTICIPANT,
        _zkAddr);
    _job = job;
  }

  public void resetTransition()
  {
    _msModelFacotry.resetTrasition();
  }
  
  public ClusterManager getManager()
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
      StateMachEngine stateMach = _manager.getStateMachineEngine();
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
