package com.linkedin.clustermanager.store.zk;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

public class ZKClientFactory
{
  private final int _sessionTimeout = 4000;
  private final int _connectTimeout = 10000;

  // private Map<String, ZkConnection> _zkConnMap;

  public ZKClientFactory()
  {
    // _zkConnMap = new ConcurrentHashMap<String, ZkConnection>();
  }

  public synchronized ZkClient create(String zkServers)
  {
    String[] zkServerArr = zkServers.split(",");

    // FIXIT: can't reuse a zk-connection!
    /**
    for (String zkServer : zkServerArr)
    {
      ZkConnection zkConnection = _zkConnMap.get(zkServer);
      if (zkConnection != null)
        return new ZkClient(zkConnection);
    }
    **/
    
    ZkConnection zkConnection = new ZkConnection(zkServers, _sessionTimeout);

    
    // TODO: change the serializer
    ZkClient client = new ZkClient(zkConnection);
    
    /**
    for (String zkServer : zkServerArr)
    {
      _zkConnMap.put(zkServer, zkConnection);
    }
    **/
    
    return client;
  }

  public static void main(String[] args) throws Exception
  {
    ZKClientFactory clientFactory = new ZKClientFactory();

    String zkServers = "localhost:2188";
    ZkClient client = clientFactory.create(zkServers);

    String testRoot = "/testPath1";
    // client.readData(testRoot);
    client.writeData(testRoot, "testData1");

    // client.writeData(testRoot, client);
  }
}
