package com.linkedin.helix.tools;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

/**
 * Provides ability to start zookeeper locally on a particular port
 * 
 * @author kgopalak
 * 
 */
public class LocalZKServer
{
  public void start(int port, String dataDir, String logDir) throws Exception
  {

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {

      @Override
      public void createDefaultNameSpace(ZkClient zkClient)
      {

      }
    };
    ZkServer server = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    server.start();
    Thread.currentThread().join();
  }

  public static void main(String[] args) throws Exception
  {
    int port = 2199;
    String rootDir = System.getProperty("java.io.tmpdir") + "/zk-helix/"
        + System.currentTimeMillis();
    String dataDir = rootDir + "/dataDir";
    String logDir = rootDir + "/logDir";

    if (args.length > 0)
    {
      port = Integer.parseInt(args[0]);
    }
    if (args.length > 1)
    {
      dataDir = args[1];
      logDir = args[1];
    }

    if (args.length > 2)
    {
      logDir = args[2];
    }
    System.out.println("Starting Zookeeper locally at port:" + port
        + " dataDir:" + dataDir + " logDir:" + logDir);
    LocalZKServer localZKServer = new LocalZKServer();
    
    localZKServer.start(port, dataDir, logDir);
  }
}
