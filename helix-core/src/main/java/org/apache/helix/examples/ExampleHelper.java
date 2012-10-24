package org.apache.helix.examples;

import java.io.File;
import java.io.IOException;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;

public class ExampleHelper
{
  
  
  public static ZkServer startZkServer(String zkAddr)
  {
    System.out.println("Start zookeeper at " + zkAddr + " in thread "
        + Thread.currentThread().getName());

    String zkDir = zkAddr.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";
    try
    {
      FileUtils.deleteDirectory(new File(dataDir));
      FileUtils.deleteDirectory(new File(logDir));
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
      {
        // do nothing
      }
    };

    int port = Integer.parseInt(zkAddr.substring(zkAddr.lastIndexOf(':') + 1));
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();

    return zkServer;
  }
  
  public static void stopZkServer(ZkServer zkServer)
  {
    if (zkServer != null)
    {
      zkServer.shutdown();
      System.out.println("Shut down zookeeper at port " + zkServer.getPort()
          + " in thread " + Thread.currentThread().getName());
    }
  }
}
