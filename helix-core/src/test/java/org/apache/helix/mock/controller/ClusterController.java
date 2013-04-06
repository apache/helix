package org.apache.helix.mock.controller;

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

import java.util.concurrent.CountDownLatch;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZkHelixTestManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.participant.DistClusterControllerStateModelFactory;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;


public class ClusterController extends Thread
{
  private static Logger        LOG                      =
                                                            Logger.getLogger(ClusterController.class);

  private final CountDownLatch _startCountDown          = new CountDownLatch(1);
  private final CountDownLatch _stopCountDown           = new CountDownLatch(1);
  private final CountDownLatch _waitStopFinishCountDown = new CountDownLatch(1);
  private final String         _controllerMode;
  private final String         _zkAddr;

  private ZkHelixTestManager   _manager;

  public ClusterController(String clusterName, String controllerName, String zkAddr) throws Exception
  {
    this(clusterName, controllerName, zkAddr, HelixControllerMain.STANDALONE.toString());
  }

  public ClusterController(String clusterName,
                           String controllerName,
                           String zkAddr,
                           String controllerMode) throws Exception
  {
    _controllerMode = controllerMode;
    _zkAddr = zkAddr;

    if (_controllerMode.equals(HelixControllerMain.STANDALONE.toString()))
    {
      _manager = new ZkHelixTestManager(clusterName, controllerName, InstanceType.CONTROLLER, zkAddr);
    }
    else if (_controllerMode.equals(HelixControllerMain.DISTRIBUTED.toString()))
    {
      _manager = new ZkHelixTestManager(clusterName, controllerName, InstanceType.CONTROLLER_PARTICIPANT, zkAddr);
    }
    else
    {
      throw new IllegalArgumentException("Controller mode: " + controllerMode
          + " NOT recoginized");
    }
  }

  public ZkHelixTestManager getManager()
  {
    return _manager;
  }

  public void syncStop()
  {
    if (_manager == null)
    {
      LOG.warn("manager already stopped");
      return;
    }

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
  }

  public void syncStart()
  {
    // TODO: prevent start multiple times
    
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
      try
      {
        if (_controllerMode.equals(HelixControllerMain.STANDALONE.toString()))
        {
          _manager.connect();
        }
        else if (_controllerMode.equals(HelixControllerMain.DISTRIBUTED.toString()))
        {
          DistClusterControllerStateModelFactory stateModelFactory =
              new DistClusterControllerStateModelFactory(_zkAddr);

          StateMachineEngine stateMach = _manager.getStateMachineEngine();
          stateMach.registerStateModelFactory("LeaderStandby", stateModelFactory);
          _manager.connect();
        }
      }
      catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      finally
      {
        _startCountDown.countDown();
        _stopCountDown.await();
      }
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    finally
    {
      synchronized (_manager)
      {
        _manager.disconnect();
        _manager = null;
      }
      _waitStopFinishCountDown.countDown();
    }
  }
}
