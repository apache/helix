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

/**
 * A top layer class that
 *  1. get event from Handler
 *  2. Maintain a gateway service registry, one gateway service maps to one Helix cluster
 *  3. On init connect, create the participant manager
 *  4. For ST reply message, update the tracker
 */

public class GatewayServiceManager {
  private Map<String, Map<String, AtomicBoolean>> _flagMap;
  private Map<String, MockApplication> _channelMap;
  HelixGatewayServiceProcessor _helixGatewayServiceProcessor;

  // event queue
  // state tracker, call tracker.update

  public GatewayServiceManager() {
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
