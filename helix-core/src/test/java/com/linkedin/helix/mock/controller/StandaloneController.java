package com.linkedin.helix.mock.controller;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;

public class StandaloneController extends Thread
{
  private static Logger        LOG             =
                                                   Logger.getLogger(StandaloneController.class);

  private final CountDownLatch _startCountDown = new CountDownLatch(1);
  private final CountDownLatch _stopCountDown  = new CountDownLatch(1);
  private final CountDownLatch _waitStopFinishCountDown  = new CountDownLatch(1);
  
  private final HelixManager   _manager;

  public StandaloneController(String clusterName, String controllerName, String zkAddr) throws Exception
  {
    _manager =
        HelixManagerFactory.getZKHelixManager(clusterName,
                                              controllerName,
                                              InstanceType.CONTROLLER,
                                              zkAddr);
  }

  public HelixManager getManager()
  {
    return _manager;
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
      _manager.connect();
      _startCountDown.countDown();
      _stopCountDown.await();
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
      }
      _waitStopFinishCountDown.countDown();
    }
  }
}
