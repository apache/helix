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
import com.linkedin.helix.model.Message.MessageType;
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
  private ClusterManager _manager;
  private final MockSMModelFactory _smModelFacotry;

  
  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE" })
  public class MockSMStateModel extends StateModel
  {
    private MockTransitionIntf _transition;
    public MockSMStateModel(MockTransitionIntf transition)
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

  public class MockSMModelFactory
    extends StateModelFactory<MockSMStateModel>
  {
    private MockTransitionIntf _transition;
    
    public MockSMModelFactory()
    {
      this(null);
    }
    
    public MockSMModelFactory(MockTransitionIntf transition)
    {
      _transition = transition;
    }
    
    public void resetTrasition()
    {
      _transition = null;
      Map<String, MockSMStateModel> stateModelMap = getStateModelMap();
      for (MockSMStateModel stateModel : stateModelMap.values())
      {
        stateModel.resetTransition();
      }
    }

    @Override
    public MockSMStateModel createNewStateModel(String partitionKey)
    {
      MockSMStateModel model = new MockSMStateModel(_transition);

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
  {
    this(clusterName, instanceName, zkAddr, null);
  }

  public MockParticipant(String clusterName, String instanceName, String zkAddr, 
                          MockTransitionIntf transition)
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _zkAddr = zkAddr;
    _smModelFacotry = new MockSMModelFactory(transition);
  }

  public void resetTransition()
  {
    _smModelFacotry.resetTrasition();
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
      _manager = ClusterManagerFactory.getZKClusterManager(_clusterName,
                                                           _instanceName,
                                                           InstanceType.PARTICIPANT,
                                                           _zkAddr);
      _manager.connect();

      StateMachineEngine stateMachine = new StateMachineEngine();
      stateMachine.registerStateModelFactory("MasterSlave", _smModelFacotry);

      DummyLeaderStandbyStateModelFactory lsModelFactory = new DummyLeaderStandbyStateModelFactory(10);
      DummyOnlineOfflineStateModelFactory ofModelFactory = new DummyOnlineOfflineStateModelFactory(10);
      stateMachine.registerStateModelFactory("LeaderStandby", lsModelFactory);
      stateMachine.registerStateModelFactory("OnlineOffline", ofModelFactory);

      _manager.getMessagingService()
              .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
                                             stateMachine);

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
