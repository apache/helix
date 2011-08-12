package com.linkedin.clustermanager;

import java.io.File;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

public class TestHelper
{
  static public ZkServer startZkSever(final String zkAddress, final String rootNamespace)
  {
    final String logDir = "/tmp/logs";
    final String dataDir = "/tmp/dataDir";
    new File(dataDir).delete();
    
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient)
      {
        zkClient.deleteRecursive(rootNamespace);
        zkClient.createPersistent(rootNamespace);
      }
    };
   
    int port = Integer.parseInt( zkAddress.substring(zkAddress.lastIndexOf(':')+1, zkAddress.length()) );
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();
    
    return zkServer;
  }
  
  static public void stopZkServer(ZkServer zkServer)
  {
    if (zkServer != null)
    {
      zkServer.shutdown();
    }
  }
  

}
