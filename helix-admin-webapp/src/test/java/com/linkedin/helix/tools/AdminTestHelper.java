package com.linkedin.helix.tools;

import java.util.concurrent.CountDownLatch;

import com.linkedin.helix.webapp.HelixAdminWebApp;

public class AdminTestHelper
{

  public static class AdminThread
  {
    Thread _adminThread;
    CountDownLatch _stopCountDown = new CountDownLatch(1);
    String _zkAddr;
    int _port;
    
    public AdminThread(String zkAddr, int port)
    {
      _zkAddr = zkAddr;
      _port = port;
    }
    
    public void start()
    {
      Thread adminThread = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          HelixAdminWebApp app = null;
          try
          {
            app = new HelixAdminWebApp(_zkAddr, _port);
            app.start();
            // Thread.currentThread().join();
            _stopCountDown.await();
          }
          catch (Exception e)
          {
            e.printStackTrace();
          }
          finally
          {
            if (app != null)
            {
              app.stop();
            }
          }
        }
      });

      adminThread.setDaemon(true);
      adminThread.start();
    }
    
    public void stop()
    {
      _stopCountDown.countDown();
    }
  }
  
}
