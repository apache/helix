package org.apache.helix.tools;

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

import org.apache.helix.webapp.HelixAdminWebApp;


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
//              System.err.println("Stopping HelixAdminWebApp");
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
