package org.apache.helix.gateway.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;

import org.apache.helix.gateway.service.ClusterManager;

public class MockApplication {
  private final ClusterManager _clusterManager;
  private Map<String, Map<String, String>> _currentStates;
  private String _instanceName;
  private String _clusterName;
  private Queue<MockProtoRequest> _requestQueue;

  public MockApplication(String instanceName, String clusterName, ClusterManager clusterManager) {
    _instanceName = instanceName;
    _clusterName = clusterName;
    _currentStates = new HashMap<>();
    _requestQueue = new LinkedList<>();
    _clusterManager = clusterManager;
    Executors.newScheduledThreadPool(1)
        .scheduleAtFixedRate(this::process, 0, 5000, java.util.concurrent.TimeUnit.MILLISECONDS);
  }

  public void process() {
    List<MockProtoResponse> completedMessages = new ArrayList<>();
    synchronized (_requestQueue) {
      while (!_requestQueue.isEmpty()) {
        MockProtoRequest request = _requestQueue.poll();
        switch (request.getMessageType()) {
          case ADD:
            addShard(request.getResourceName(), request.getShardName());
            completedMessages.add(new MockProtoResponse(request.getMessageId()));
            break;
          case REMOVE:
            removeShard(request.getResourceName(), request.getShardName());
            completedMessages.add(new MockProtoResponse(request.getMessageId()));
            break;
          case CHANGE_ROLE:
            changeRole(request.getResourceName(), request.getShardName(), request.getFromState(),
                request.getToState());
            completedMessages.add(new MockProtoResponse(request.getMessageId()));
            break;
          default:
            System.out.println("Unknown message type: " + request.getMessageType());
            throw new RuntimeException("Unknown message type: " + request.getMessageType());
        }
      }
    }
    _clusterManager.receiveResponse(completedMessages, _instanceName);
  }

  public void addRequest(MockProtoRequest request) {
    synchronized (_requestQueue) {
      _requestQueue.add(request);
    }
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public void join() {
    System.out.println(
        "Joining Mock Application for instance " + _instanceName + " in cluster " + _clusterName);
  }

  public synchronized void addShard(String resourceName, String shardName) {
    System.out.println("ADD | " + shardName + " | " + resourceName + " | " + _instanceName);
  }

  public synchronized void removeShard(String resourceName, String shardName) {
    System.out.println("REMOVE | " + shardName + " | " + resourceName + " | " + _instanceName);
  }

  public synchronized void changeRole(String resourceName, String shardName, String fromState,
      String toState) {
    System.out.println(
        "CHANGE ROLE | " + shardName + " | " + resourceName + " | " + _instanceName + " | "
            + fromState + " -> " + toState);
    _currentStates.computeIfAbsent(resourceName, k -> new HashMap<>()).put(shardName, toState);
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
