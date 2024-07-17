package org.apache.helix.gateway.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.gateway.mock.MockApplication;
import org.apache.helix.gateway.mock.MockProtoRequest;
import org.apache.helix.gateway.mock.MockProtoResponse;

public class ClusterManager {
  private Map<String, Map<String, AtomicBoolean>> _flagMap;
  private Map<String, MockApplication> _channelMap;
  private Lock _lock = new ReentrantLock();

  // event queue
  // state tracker, call tracker.update

  public ClusterManager() {
    _flagMap = new ConcurrentHashMap<>();
    _channelMap = new ConcurrentHashMap<>();
  }

  public void addChannel(MockApplication mockApplication) {
    _channelMap.put(mockApplication.getInstanceName(), mockApplication);
    _flagMap.computeIfAbsent(mockApplication.getInstanceName(), k -> new ConcurrentHashMap<>());
  }

  public void removeChannel(String instanceName) {
    _channelMap.remove(instanceName);
    _flagMap.remove(instanceName);
  }

  public AtomicBoolean sendMessage(MockProtoRequest request) {
    MockApplication mockApplication = _channelMap.get(request.getInstanceName());
    synchronized (mockApplication) {
      mockApplication.addRequest(request);
      AtomicBoolean flag = new AtomicBoolean(false);
      _flagMap.computeIfAbsent(request.getInstanceName(), k -> new ConcurrentHashMap<>())
          .put(request.getMessageId(), flag);
      return flag;
    }
  }

  public synchronized void receiveResponse(List<MockProtoResponse> responses, String instanceName) {
    for (MockProtoResponse response : responses) {
      AtomicBoolean flag = _flagMap.get(instanceName).remove(response.getMessageId());
      flag.set(true);
    }
  }
}
