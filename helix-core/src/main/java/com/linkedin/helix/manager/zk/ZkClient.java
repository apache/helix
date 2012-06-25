/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.manager.zk;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.CreateCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.DeleteCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.ExistsCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.GetDataCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.SetDataCallbackHandler;

/**
 * ZKClient does not provide some functionalities, this will be used for quick fixes if
 * any bug found in ZKClient or if we need additional features but can't wait for the new
 * ZkClient jar Ideally we should commit the changes we do here to ZKClient.
 * 
 * @author kgopalak
 * 
 */

public class ZkClient extends org.I0Itec.zkclient.ZkClient
{
  private static Logger                   LOG                        =
                                                                         Logger.getLogger(ZkClient.class);
  public static final int                 DEFAULT_CONNECTION_TIMEOUT = 10000;
  public static String                    sessionId;
  public static String                    sessionPassword;

  // TODO need to remove when connection expired
  private static final Set<IZkConnection> zkConnections              =
                                                                         new CopyOnWriteArraySet<IZkConnection>();
  private ZkSerializer                    _zkSerializer;

  public ZkClient(IZkConnection connection,
                  int connectionTimeout,
                  ZkSerializer zkSerializer)
  {
    super(connection, connectionTimeout, zkSerializer);
    _zkSerializer = zkSerializer;
    zkConnections.add(_connection);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout)
  {
    this(connection, connectionTimeout, new SerializableSerializer());
  }

  public ZkClient(IZkConnection connection)
  {
    this(connection, Integer.MAX_VALUE, new SerializableSerializer());
  }

  public ZkClient(String zkServers,
                  int sessionTimeout,
                  int connectionTimeout,
                  ZkSerializer zkSerializer)
  {
    this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout)
  {
    this(new ZkConnection(zkServers, sessionTimeout),
         connectionTimeout,
         new SerializableSerializer());
  }

  public ZkClient(String zkServers, int connectionTimeout)
  {
    this(new ZkConnection(zkServers), connectionTimeout, new SerializableSerializer());
  }

  public ZkClient(String zkServers)
  {
    this(new ZkConnection(zkServers), Integer.MAX_VALUE, new SerializableSerializer());
  }

  @Override
  public void setZkSerializer(ZkSerializer zkSerializer)
  {
    super.setZkSerializer(zkSerializer);
    _zkSerializer = zkSerializer;
  }

  public ZkSerializer getZkSerializer()
  {
    return _zkSerializer;
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
        Stat stat = ((ZkConnection) _connection).getZookeeper().exists(path, false);
        return stat;
      }
    });

    return stat;
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readDataAndStat(String path,
                                              Stat stat,
                                              boolean returnNullIfPathNotExists)
  {
    T data = null;
    try
    {
      data = (T) super.readData(path, stat);
    }
    catch (ZkNoNodeException e)
    {
      if (!returnNullIfPathNotExists)
      {
        throw e;
      }
    }
    return data;
  }

  public String getServers()
  {
    return _connection.getServers();
  }

  // TODO: remove this
  class LogStatCallback implements StatCallback
  {
    final String _asyncMethodName;

    public LogStatCallback(String asyncMethodName)
    {
      _asyncMethodName = asyncMethodName;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat)
    {
      if (rc == 0)
      {
        LOG.info("succeed in async " + _asyncMethodName + ". rc: " + rc + ", path: "
            + path + ", stat: " + stat);
      }
      else
      {
        LOG.error("fail in async " + _asyncMethodName + ". rc: " + rc + ", path: " + path
            + ", stat: " + stat);
      }
    }

  }

  // TODO: remove this
  public void asyncWriteData(final String path, Object datat)
  {
    Stat stat = getStat(path);
    this.asyncWriteData(path,
                        datat,
                        stat.getVersion(),
                        new LogStatCallback("asyncSetData"),
                        null);
  }

  // TODO: remove this
  public void asyncWriteData(final String path,
                             Object datat,
                             final int version,
                             final StatCallback cb,
                             final Object ctx)
  {
    final byte[] data = _zkSerializer.serialize(datat);
    ((ZkConnection) _connection).getZookeeper().setData(path, data, version, cb, ctx);

  }

  public void asyncCreate(final String path,
                          Object datat,
                          CreateMode mode,
                          CreateCallbackHandler cb)
  {
    final byte[] data = _zkSerializer.serialize(datat);
    ((ZkConnection) _connection).getZookeeper().create(path,
                                                       data,
                                                       Ids.OPEN_ACL_UNSAFE, // Arrays.asList(DEFAULT_ACL),
                                                       mode,
                                                       cb,
                                                       null);
  }

  public void asyncSetData(final String path,
                           Object datat,
                           int version,
                           SetDataCallbackHandler cb)
  {
    final byte[] data = _zkSerializer.serialize(datat);
    ((ZkConnection) _connection).getZookeeper().setData(path, data, version, cb, null);

  }

  public void asyncGetData(final String path, GetDataCallbackHandler cb)
  {
    ((ZkConnection) _connection).getZookeeper().getData(path, null, cb, null);
  }

  public void asyncExists(final String path, ExistsCallbackHandler cb)
  {
    ((ZkConnection) _connection).getZookeeper().exists(path, null, cb, null);

  }

  public void asyncDelete(String path, DeleteCallbackHandler cb)
  {
    ((ZkConnection) _connection).getZookeeper().delete(path, -1, cb, null);
  }

}
