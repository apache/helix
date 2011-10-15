package com.linkedin.clustermanager.agent.zk;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
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
  public static String sessionId;
  public static String sessionPassword;
  // TODO need to remove when connection expired
  private static final Set<IZkConnection> zkConnections = new CopyOnWriteArraySet<IZkConnection>();
  
  public ZkClient(IZkConnection connection, int connectionTimeout,
      ZkSerializer zkSerializer)
  {
    super(connection, connectionTimeout, zkSerializer);
    zkConnections.add(_connection);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout)
  {
    super(connection, connectionTimeout);
    zkConnections.add(_connection);
  }

  public ZkClient(IZkConnection connection)
  {
    super(connection);
    zkConnections.add(_connection);
  }

  public ZkClient(String zkServers, int sessionTimeout,
      int connectionTimeout, ZkSerializer zkSerializer)
  {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer);
    zkConnections.add(_connection);
  }

  public ZkClient(String zkServers, int sessionTimeout,
      int connectionTimeout)
  {
    super(zkServers, sessionTimeout, connectionTimeout);
    zkConnections.add(_connection);
  }

  public ZkClient(String zkServers, int connectionTimeout)
  {
    super(zkServers, connectionTimeout);
    zkConnections.add(_connection);
  }

  public ZkClient(String serverstring)
  {
    super(serverstring);
    zkConnections.add(_connection);
  }

  public IZkConnection getConnection()
  {
  	return _connection;
  }

  @Override
  public void close() throws ZkInterruptedException 
  {
    zkConnections.remove(_connection);
    super.close();
  }
  
  public static int getNumberOfConnections()
  {
    return zkConnections.size();
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
  
  @Override
  public <T extends Object> T readData(String path, boolean returnNullIfPathNotExists) 
  {
    T data = null;
    try {
        data = (T) readData(path, null);
    } catch (ZkNoNodeException e) {
        if (!returnNullIfPathNotExists) {
            throw e;
        }
    }
    return data;
  }
}
