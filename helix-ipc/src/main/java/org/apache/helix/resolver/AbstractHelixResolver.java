package org.apache.helix.resolver;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A basic implementation of a resolver in terms of expiring routing tables
 */
public abstract class AbstractHelixResolver implements HelixResolver {
  private static final Logger LOG = Logger.getLogger(AbstractHelixResolver.class);
  private static final int DEFAULT_THREAD_POOL_SIZE = 10;
  private static final long DEFAULT_LEASE_LENGTH_MS = 60 * 60 * 1000; // TODO: are these good
                                                                      // values?
  private static final String IPC_PORT = "IPC_PORT";
  private final Map<String, Spectator> _connections;
  private boolean _isConnected;
  private ScheduledExecutorService _executor;

  protected AbstractHelixResolver() {
    _connections = Maps.newHashMap();
    _isConnected = false;
  }

  @Override
  public void connect() {
    _executor = Executors.newScheduledThreadPool(DEFAULT_THREAD_POOL_SIZE);
    _isConnected = true;
  }

  @Override
  public void disconnect() {
    synchronized (_connections) {
      for (Spectator connection : _connections.values()) {
        connection.shutdown();
      }
      _connections.clear();
    }
    _executor.shutdown();
    _isConnected = false;
  }

  @Override
  public Set<HelixAddress> getDestinations(HelixMessageScope scope) {
    if (!scope.isValid()) {
      LOG.error("Scope " + scope + " is not valid!");
      return new HashSet<HelixAddress>();
    } else if (!_isConnected) {
      LOG.error("Cannot resolve " + scope + " without first connecting!");
      return new HashSet<HelixAddress>();
    }

    // Connect or refresh connection
    String cluster = scope.getCluster();
    ResolverRoutingTable routingTable;
    Spectator connection = _connections.get(cluster);
    if (connection == null || !connection.getManager().isConnected()) {
      synchronized (_connections) {
        connection = _connections.get(cluster);
        if (connection == null || !connection.getManager().isConnected()) {
          connection = new Spectator(cluster, DEFAULT_LEASE_LENGTH_MS);
          connection.init();
          _connections.put(cluster, connection);
        }
      }
    }
    routingTable = connection.getRoutingTable();

    // Resolve all resources, either explicitly or match all
    Set<String> resources;
    if (scope.getResource() != null) {
      resources = Sets.newHashSet(scope.getResource());
    } else {
      resources = routingTable.getResources();
    }

    // Resolve all partitions
    Map<String, Set<String>> partitionMap = Maps.newHashMap();
    if (scope.getPartition() != null) {
      for (String resource : resources) {
        partitionMap.put(resource, Sets.newHashSet(scope.getPartition()));
      }
    } else {
      for (String resource : resources) {
        partitionMap.put(resource, routingTable.getPartitions(resource));
      }
    }

    // Resolve all states
    Set<String> states;
    if (scope.getState() != null) {
      states = Sets.newHashSet(scope.getState());
    } else {
      states = routingTable.getStates();
    }

    // Get all the participants that match
    Set<InstanceConfig> participants = Sets.newHashSet();
    for (String resource : resources) {
      for (String partition : partitionMap.get(resource)) {
        for (String state : states) {
          participants.addAll(routingTable.getInstances(resource, partition, state));
        }
      }
    }

    // Resolve those participants
    Set<HelixAddress> result = new HashSet<HelixAddress>();
    for (InstanceConfig participant : participants) {
      String ipcPort = participant.getRecord().getSimpleField(IPC_PORT);
      if (ipcPort == null) {
        LOG.error("No ipc address registered for target instance " + participant.getInstanceName()
            + ", skipping");
      } else {
        result.add(new HelixAddress(scope, participant.getInstanceName(), new InetSocketAddress(
            participant.getHostName(), Integer.valueOf(ipcPort))));
      }
    }

    return result;
  }

  @Override
  public HelixAddress getSource(HelixMessageScope scope) {
    // Connect or refresh connection
    String cluster = scope.getCluster();
    ResolverRoutingTable routingTable;
    Spectator connection = _connections.get(cluster);
    if (connection == null || !connection.getManager().isConnected()) {
      synchronized (_connections) {
        connection = _connections.get(cluster);
        if (connection == null || !connection.getManager().isConnected()) {
          connection = new Spectator(cluster, DEFAULT_LEASE_LENGTH_MS);
          connection.init();
          _connections.put(cluster, connection);
        }
      }
    }
    routingTable = connection.getRoutingTable();

    if (scope.getSourceInstance() != null) {
      InstanceConfig config = routingTable.getInstanceConfig(scope.getSourceInstance());
      String ipcPort = config.getRecord().getSimpleField(IPC_PORT);
      if (ipcPort == null) {
        throw new IllegalStateException("No IPC address registered for source instance "
            + scope.getSourceInstance());
      }
      return new HelixAddress(scope, scope.getSourceInstance(), new InetSocketAddress(
          config.getHostName(), Integer.valueOf(ipcPort)));
    }

    return null;
  }

  @Override
  public boolean isConnected() {
    return _isConnected;
  }

  /**
   * Create a Helix manager connection based on the appropriate backing store
   * @param cluster the name of the cluster to connect to
   * @return HelixManager instance
   */
  protected abstract HelixManager createManager(String cluster);

  private class Spectator {
    private final String _cluster;
    private final HelixManager _manager;
    private final ResolverRoutingTable _routingTable;
    private final long _leaseLengthMs;
    private ScheduledFuture<?> _future;

    /**
     * Initialize a spectator. This does not automatically connect.
     * @param cluster the cluster to spectate
     * @param leaseLengthMs the expiry of this spectator after the last request
     */
    public Spectator(String cluster, long leaseLengthMs) {
      _cluster = cluster;
      _manager = createManager(cluster);
      _leaseLengthMs = leaseLengthMs;
      _routingTable = new ResolverRoutingTable();
    }

    /**
     * Connect and initialize the routing table
     */
    public void init() {
      try {
        _manager.connect();
        _manager.addExternalViewChangeListener(_routingTable);
        _manager.addInstanceConfigChangeListener(_routingTable);

        // Force an initial refresh
        HelixDataAccessor accessor = _manager.getHelixDataAccessor();
        List<ExternalView> externalViews =
            accessor.getChildValues(accessor.keyBuilder().externalViews());
        NotificationContext context = new NotificationContext(_manager);
        context.setType(NotificationContext.Type.INIT);
        _routingTable.onExternalViewChange(externalViews, context);
        List<InstanceConfig> instanceConfigs =
            accessor.getChildValues(accessor.keyBuilder().instanceConfigs());
        _routingTable.onInstanceConfigChange(instanceConfigs, context);
      } catch (Exception e) {
        LOG.error("Error setting up routing table", e);
      }
    }

    /**
     * Clean up the connection to the spectated cluster
     */
    public void shutdown() {
      resetFuture();
      expire();
    }

    /**
     * Get the dynamically-updating routing table for this cluster
     * @return ResolverRoutingTable, a RoutingTableProvider that can answer questions about its
     *         contents
     */
    public ResolverRoutingTable getRoutingTable() {
      renew();
      return _routingTable;
    }

    public HelixManager getManager() {
      return _manager;
    }

    private synchronized void renew() {
      resetFuture();

      // Schedule this connection to expire if not renewed quickly enough
      _future = _executor.schedule(new Runnable() {
        @Override
        public void run() {
          expire();
        }
      }, _leaseLengthMs, TimeUnit.MILLISECONDS);

    }

    private synchronized void resetFuture() {
      if (_future != null && !_future.isDone()) {
        _future.cancel(true);
      }
    }

    private void expire() {
      synchronized (_connections) {
        _connections.remove(_cluster);
        if (_manager != null && _manager.isConnected()) {
          _manager.disconnect();
        }
      }
    }
  }
}
