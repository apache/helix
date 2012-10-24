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
package org.apache.helix;

import org.apache.helix.HelixManager;
import org.apache.helix.mock.storage.DummyProcess.DummyLeaderStandbyStateModelFactory;
import org.apache.helix.mock.storage.DummyProcess.DummyOnlineOfflineStateModelFactory;
import org.apache.helix.mock.storage.DummyProcess.DummyStateModelFactory;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;


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
