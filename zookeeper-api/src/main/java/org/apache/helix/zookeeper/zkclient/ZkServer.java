package org.apache.helix.zookeeper.zkclient;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkServer {

    private static Logger LOG = LoggerFactory.getLogger(ZkServer.class);

    public static final int DEFAULT_PORT = 2181;
    public static final int DEFAULT_TICK_TIME = 5000;
    public static final int DEFAULT_MIN_SESSION_TIMEOUT = 2 * DEFAULT_TICK_TIME;

    private String _dataDir;
    private String _logDir;

    private IDefaultNameSpace _defaultNameSpace;

    private ZooKeeperServer _zk;
    private NIOServerCnxnFactory _nioFactory;
    private ZkClient _zkClient;
    private int _port;
    private int _tickTime;
    private int _minSessionTimeout;

    public ZkServer(String dataDir, String logDir, IDefaultNameSpace defaultNameSpace) {
        this(dataDir, logDir, defaultNameSpace, DEFAULT_PORT);
    }

    public ZkServer(String dataDir, String logDir, IDefaultNameSpace defaultNameSpace, int port) {
        this(dataDir, logDir, defaultNameSpace, port, DEFAULT_TICK_TIME);
    }

    public ZkServer(String dataDir, String logDir, IDefaultNameSpace defaultNameSpace, int port, int tickTime) {
        this(dataDir, logDir, defaultNameSpace, port, tickTime, DEFAULT_MIN_SESSION_TIMEOUT);
    }

    public ZkServer(String dataDir, String logDir, IDefaultNameSpace defaultNameSpace, int port, int tickTime, int minSessionTimeout) {
        _dataDir = dataDir;
        _logDir = logDir;
        _defaultNameSpace = defaultNameSpace;
        _port = port;
        _tickTime = tickTime;
        _minSessionTimeout = minSessionTimeout;
    }

    public int getPort() {
        return _port;
    }

    @PostConstruct
    public void start() {
        startZooKeeperServer();
        _zkClient = new ZkClient(new ZkConnection("localhost:" + _port), 10000, -1, new BasicZkSerializer(new SerializableSerializer()), null, null, null, false);
        _defaultNameSpace.createDefaultNameSpace(_zkClient);
    }

    private void startZooKeeperServer() {
        long startTime = System.currentTimeMillis();
        /**
         * Checking if the ZK hostname is present in the list of resolved ips/hostnames for
         * all network interfaces on a machine is expensive. So, we start the ZK on any local
         * address and just check if the port specified is free.
         */
        if (NetworkUtil.isPortFree(_port)) {
            final File dataDir = new File(_dataDir);
            final File dataLogDir = new File(_logDir);
            dataDir.mkdirs();
            dataLogDir.mkdirs();
            LOG.info(String.format("Starting single zookeeper server on port %d...", _port));
            LOG.info("data dir: " + dataDir.getAbsolutePath());
            LOG.info("data log dir: " + dataLogDir.getAbsolutePath());
            startSingleZkServer(_tickTime, dataDir, dataLogDir, _port);
            LOG.info(String.format("Started single zookeeper server on port %d in %d milliseconds", _port, System.currentTimeMillis() - startTime));
        } else {
            throw new IllegalStateException("Zookeeper port " + _port + " was already in use. Running in single machine mode?");
        }
    }

    private void startSingleZkServer(final int tickTime, final File dataDir, final File dataLogDir, final int port) {
        try {
            _zk = new ZooKeeperServer(dataDir, dataLogDir, tickTime);
            _zk.setMinSessionTimeout(_minSessionTimeout);
            _nioFactory = new NIOServerCnxnFactory();
            int maxClientConnections = 0; // 0 means unlimited
            _nioFactory.configure(new InetSocketAddress(port), maxClientConnections);
            _nioFactory.startup(_zk);
        } catch (IOException e) {
            throw new ZkException("Unable to start single ZooKeeper server.", e);
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        }
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down ZkServer...");
        try {
            _zkClient.close();
        } catch (ZkException e) {
            LOG.warn("Error on closing zkclient: " + e.getClass().getName());
        }
        if (_nioFactory != null) {
            _nioFactory.shutdown();
            try {
                _nioFactory.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            _nioFactory = null;
        }
        if (_zk != null) {
            _zk.shutdown(true);
            _zk = null;
        }
        LOG.info("Shutting down ZkServer...done");
    }

    public ZkClient getZkClient() {
        return _zkClient;
    }

    /**
     * Get the ZooKeeper server instance.
     * @return The ZooKeeper server instance
     */
    public ZooKeeperServer getZooKeeperServer() {
        return _zk;
    }

}
