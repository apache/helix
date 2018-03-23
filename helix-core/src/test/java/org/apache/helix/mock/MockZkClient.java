package org.apache.helix.mock;

import java.util.HashMap;
import java.util.Map;
import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.manager.zk.PathBasedZkSerializer;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkAsyncCallbacks;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.zookeeper.KeeperException;

public class MockZkClient extends ZkClient {
  Map<String, byte[]> _dataMap;

  public MockZkClient(String zkAddress) {
    super(zkAddress);
    _dataMap = new HashMap<>();
    setZkSerializer(new ZNRecordSerializer());
  }
  public MockZkClient(IZkConnection zkConnection, int connectionTimeout, long operationRetryTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey,
      String monitorInstanceName, boolean monitorRootPathOnly) {
    super(zkConnection, connectionTimeout, operationRetryTimeout, zkSerializer, monitorType,
        monitorKey, monitorInstanceName, monitorRootPathOnly);
  }

  public MockZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey,
      long operationRetryTimeout) {
    super(connection, connectionTimeout, zkSerializer, monitorType, monitorKey,
        operationRetryTimeout);
  }

  public MockZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey) {
    super(connection, connectionTimeout, zkSerializer, monitorType, monitorKey);
  }

  public MockZkClient(String zkServers, String monitorType, String monitorKey) {
    super(zkServers, monitorType, monitorKey);
  }

  public MockZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey) {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer, monitorType, monitorKey);
  }

  public MockZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer) {
    super(connection, connectionTimeout, zkSerializer);
  }

  public MockZkClient(IZkConnection connection, int connectionTimeout, ZkSerializer zkSerializer) {
    super(connection, connectionTimeout, zkSerializer);
  }

  public MockZkClient(IZkConnection connection, int connectionTimeout) {
    super(connection, connectionTimeout);
  }

  public MockZkClient(IZkConnection connection) {
    super(connection);
  }

  public MockZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      ZkSerializer zkSerializer) {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer);
  }

  public MockZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      PathBasedZkSerializer zkSerializer) {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer);
  }

  public MockZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
    super(zkServers, sessionTimeout, connectionTimeout);
  }

  public MockZkClient(String zkServers, int connectionTimeout) {
    super(zkServers, connectionTimeout);
  }

  public MockZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      ZkSerializer zkSerializer, long operationRetryTimeout) {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer, operationRetryTimeout);
  }

  public MockZkClient(IZkConnection zkConnection, int connectionTimeout, ZkSerializer zkSerializer,
      long operationRetryTimeout) {
    super(zkConnection, connectionTimeout, zkSerializer, operationRetryTimeout);
  }


  @Override
  public void asyncGetData(final String path,
      final ZkAsyncCallbacks.GetDataCallbackHandler cb) {
    if (_dataMap.containsKey(path)) {
      if (_dataMap.get(path) == null) {
        cb.processResult(4, path, null, _dataMap.get(path), null);
      } else {
        cb.processResult(0, path, null, _dataMap.get(path), null);
      }
    } else {
      cb.processResult(KeeperException.Code.NONODE.intValue(), path, null, null, null);
    }
  }

  public void putData(String path, byte[] data) {
    _dataMap.put(path, data);
  }

  public byte[] removeData(String path) {
    return _dataMap.remove(path);
  }
}
