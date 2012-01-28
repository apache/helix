package com.linkedin.clustermanager;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyLeaderStandbyStateModelFactory;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyOnlineOfflineStateModelFactory;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyStateModelFactory;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;

public class DummyProcessThread implements Runnable
{
  private static final Logger LOG = Logger.getLogger(DummyProcessThread.class);

  ClusterManager _manager;
  String _instanceName;

  public DummyProcessThread(ClusterManager manager, String instanceName)
  {
    _manager = manager;
    _instanceName = instanceName;
  }

  @Override
  public void run()
  {
    try
    {
      _manager.connect();
      DummyStateModelFactory stateModelFactory = new DummyStateModelFactory(0);
      StateMachineEngine genericStateMachineHandler =
          new StateMachineEngine();
      genericStateMachineHandler.registerStateModelFactory("MasterSlave", stateModelFactory);

      DummyLeaderStandbyStateModelFactory stateModelFactory1 = new DummyLeaderStandbyStateModelFactory(10);
      DummyOnlineOfflineStateModelFactory stateModelFactory2 = new DummyOnlineOfflineStateModelFactory(10);
      genericStateMachineHandler.registerStateModelFactory("LeaderStandby", stateModelFactory1);
      genericStateMachineHandler.registerStateModelFactory("OnlineOffline", stateModelFactory2);
      _manager.getMessagingService()
              .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
                                             genericStateMachineHandler);

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
