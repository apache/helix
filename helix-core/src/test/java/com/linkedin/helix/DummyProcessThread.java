package com.linkedin.helix;

import org.apache.log4j.Logger;

import com.linkedin.helix.mock.storage.DummyProcess.DummyLeaderStandbyStateModelFactory;
import com.linkedin.helix.mock.storage.DummyProcess.DummyOnlineOfflineStateModelFactory;
import com.linkedin.helix.mock.storage.DummyProcess.DummyStateModelFactory;
import com.linkedin.helix.participant.StateMachineEngine;

public class DummyProcessThread implements Runnable
{
  private static final Logger LOG = Logger.getLogger(DummyProcessThread.class);

  HelixManager _manager;
  String _instanceName;

  public DummyProcessThread(HelixManager manager, String instanceName)
  {
    _manager = manager;
    _instanceName = instanceName;
  }

  @Override
  public void run()
  {
    try
    {
      DummyStateModelFactory stateModelFactory = new DummyStateModelFactory(0);
//      StateMachineEngine genericStateMachineHandler =
//          new StateMachineEngine();
      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      stateMach.registerStateModelFactory("MasterSlave", stateModelFactory);

      DummyLeaderStandbyStateModelFactory stateModelFactory1 = new DummyLeaderStandbyStateModelFactory(10);
      DummyOnlineOfflineStateModelFactory stateModelFactory2 = new DummyOnlineOfflineStateModelFactory(10);
      stateMach.registerStateModelFactory("LeaderStandby", stateModelFactory1);
      stateMach.registerStateModelFactory("OnlineOffline", stateModelFactory2);
//      _manager.getMessagingService()
//              .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
//                                             genericStateMachineHandler);

      _manager.connect();
      Thread.currentThread().join();
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
