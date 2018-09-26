package org.apache.helix.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkAsyncCallbacks;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;

public class MockZkClient extends ZkClient implements HelixZkClient {
  Map<String, byte[]> _dataMap;

  public MockZkClient(String zkAddress) {
    super(zkAddress);
    _dataMap = new HashMap<>();
    setZkSerializer(new ZNRecordSerializer());
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
      super.asyncGetData(path, cb);
    }
  }

  public List<String> getChildren(final String path) {
    List<String> children = super.getChildren(path);
    for (String p : _dataMap.keySet()) {
      if (p.contains(path)) {
        String[] paths = p.split("/");
        children.add(paths[paths.length-1]);
      }
    }

    return children;
  }

  public void putData(String path, byte[] data) {
    _dataMap.put(path, data);
  }

  public byte[] removeData(String path) {
    return _dataMap.remove(path);
  }
}
