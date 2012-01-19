package com.linkedin.clustermanager.mock.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyLeaderStandbyStateModelFactory;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyOnlineOfflineStateModelFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;
import com.linkedin.clustermanager.participant.statemachine.StateModelInfo;
import com.linkedin.clustermanager.participant.statemachine.Transition;

public class ErroneousDummyParticipant implements Stoppable, Runnable
{
  private static Logger LOG = Logger.getLogger(ErroneousDummyParticipant.class);
  private final String _clusterName;
  private final String _instanceName;
  private final String _zkAddr;

  private final CountDownLatch _countDown = new CountDownLatch(1);
  private ClusterManager _manager;
  private final ErroneousDummyStateModelFactory _stateModelFacotry = new ErroneousDummyStateModelFactory();

  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE" })
  public static class ErroneousDummyStateModel extends StateModel
  {
    private final String _partition;
    private final int _delay;
    private final Set<String> _erroneousTransitions;

    public ErroneousDummyStateModel(String partition, int delay, Set<String> erroneousTransitions)
    {
      _partition = partition;
      _delay = delay;
      _erroneousTransitions = erroneousTransitions;
    }

    private void throwExceptionInTransition(String fromState, String toState)
        throws RuntimeException
    {
      String transition = fromState + "-" + toState;
      if (_erroneousTransitions.contains(transition))
      {
        String errMsg = "IGNORABLE: testing throwing exception for partition " + _partition + " transitting from "
            + fromState + " to " + toState;
        throw new RuntimeException(errMsg);
      }
    }

    @Transition(to="SLAVE",from="OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Becoming SLAVE from OFFLINE");
      throwExceptionInTransition("OFFLINE", "SLAVE");
    }

    @Transition(to="MASTER",from="SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Becoming MASTER from SLAVE");
      throwExceptionInTransition("SLAVE", "MASTER");
    }

    @Transition(to="SLAVE",from="MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Becoming SLAVE from MASTER");
      throwExceptionInTransition("MASTER", "SLAVE");
    }

    @Transition(to="OFFLINE",from="SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context)
        throws RuntimeException
    {
      LOG.info("Becoming OFFLINE from SLAVE");
      throwExceptionInTransition("SLAVE", "OFFLINE");
    }

  }

  public static class ErroneousDummyStateModelFactory
    extends StateModelFactory<ErroneousDummyStateModel>
  {
    private final Map<String, Set<String>> _erroneousTransitions = new HashMap<String, Set<String>>();

    public void setIsErroneous(String partition, String fromState, String toState,
                               boolean isErroneous)
    {
      // debug
      LOG.debug("Set " + partition + " transtion from " + fromState
              + " to " + toState + " to erroenous");

      if (!_erroneousTransitions.containsKey(partition))
      {
        _erroneousTransitions.put(partition, new HashSet<String>());
      }

      String transition = fromState + "-" + toState;
      if (isErroneous == true)
      {
        _erroneousTransitions.get(partition).add(transition);
      }
      else
      {
        _erroneousTransitions.get(partition).remove(transition);
      }
    }

    @Override
    public ErroneousDummyStateModel createNewStateModel(String resourceKey)
    {
      if (!_erroneousTransitions.containsKey(resourceKey))
      {
        _erroneousTransitions.put(resourceKey, new HashSet<String>());
      }
      ErroneousDummyStateModel model = new ErroneousDummyStateModel(resourceKey, 0,
                                                _erroneousTransitions.get(resourceKey));

      return model;
    }
  }

  public ErroneousDummyParticipant(String clusterName, String instanceName, String zkAddr)
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _zkAddr = zkAddr;
  }

  @Override
  public void stop()
  {
    _countDown.countDown();
    _manager.disconnect();
  }

  public void setIsErroneous(String partition, String fromState, String toState,
                             boolean isErroneous)
  {
    _stateModelFacotry.setIsErroneous(partition, fromState, toState, isErroneous);
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

      StateMachineEngine genericStateMachineHandler = new StateMachineEngine();
      genericStateMachineHandler.registerStateModelFactory("MasterSlave", _stateModelFacotry);

      DummyLeaderStandbyStateModelFactory leaderStandbyMF = new DummyLeaderStandbyStateModelFactory(10);
      DummyOnlineOfflineStateModelFactory onOfflineMF = new DummyOnlineOfflineStateModelFactory(10);
      genericStateMachineHandler.registerStateModelFactory("LeaderStandby", leaderStandbyMF);
      genericStateMachineHandler.registerStateModelFactory("OnlineOffline", onOfflineMF);

      _manager.getMessagingService()
              .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
                                             genericStateMachineHandler);

      _countDown.await();
    }
    catch (InterruptedException e)
    {
      String msg =
          "participant:" + _instanceName + ", " + Thread.currentThread().getName()
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
