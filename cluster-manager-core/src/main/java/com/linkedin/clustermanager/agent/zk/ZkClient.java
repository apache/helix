package com.linkedin.clustermanager.agent.zk;

import java.util.concurrent.Callable;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.data.Stat;

/**
 * ZKClient does not provide some functionalities, this will be used for quick
 * fixes if any bug found in ZKClient or if we need additional features but cant
 * wait for the new ZkClient jar
 * Ideally we should commit the changes we do here to ZKClient. 
 * @author kgopalak
 * 
 */
public class ZkClient extends org.I0Itec.zkclient.ZkClient
{

  public ZkClient(IZkConnection zkConnection, int connectionTimeout,
      ZkSerializer zkSerializer)
  {
    super(zkConnection, connectionTimeout, zkSerializer);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout)
  {
    super(connection, connectionTimeout);
  }

  public ZkClient(IZkConnection connection)
  {
    super(connection);
  }

  public ZkClient(String zkServers, int sessionTimeout,
      int connectionTimeout, ZkSerializer zkSerializer)
  {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer);
  }

  public ZkClient(String zkServers, int sessionTimeout,
      int connectionTimeout)
  {
    super(zkServers, sessionTimeout, connectionTimeout);
  }

  public ZkClient(String zkServers, int connectionTimeout)
  {
    super(zkServers, connectionTimeout);
  }

  public ZkClient(String serverstring)
  {
    super(serverstring);
  }

  public Stat getStat(final String path)
  {
    Stat stat = retryUntilConnected(new Callable<Stat>()
    {

      @Override
      public Stat call() throws Exception
      {
        Stat stat = ((ZkConnection) _connection).getZookeeper().exists(path,
            false);
        return stat;
      }
    });

    return stat;
  }
}
