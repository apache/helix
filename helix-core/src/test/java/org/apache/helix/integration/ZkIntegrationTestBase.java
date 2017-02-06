package org.apache.helix.integration;

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

import java.util.logging.Level;

import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class ZkIntegrationTestBase {
  private static Logger LOG = Logger.getLogger(ZkIntegrationTestBase.class);

  protected static ZkServer _zkServer;
  protected static ZkClient _gZkClient;
  protected static ClusterSetup _gSetupTool;
  protected static BaseDataAccessor<ZNRecord> _baseAccessor;

  public static final String ZK_ADDR = "localhost:2183";
  protected static final String CLUSTER_PREFIX = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  protected final String CONTROLLER_PREFIX = "controller";
  protected final String PARTICIPANT_PREFIX = "localhost";

  @BeforeSuite
  public void beforeSuite() throws Exception {
    // TODO: use logging.properties file to config java.util.logging.Logger levels
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);

    _zkServer = TestHelper.startZkServer(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    ZKClientPool.reset();

    _gZkClient = new ZkClient(ZK_ADDR);
    _gZkClient.setZkSerializer(new ZNRecordSerializer());
    _gSetupTool = new ClusterSetup(ZK_ADDR);
    _baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
  }

  @AfterSuite
  public void afterSuite() {
    ZKClientPool.reset();
    _gZkClient.close();
    TestHelper.stopZkServer(_zkServer);
  }

  protected String getShortClassName() {
    return this.getClass().getSimpleName();
  }

  protected String getCurrentLeader(ZkClient zkClient, String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader == null) {
      return null;
    }
    return leader.getInstanceName();
  }

  protected void enableHealthCheck(String clusterName) {
    ConfigScope scope = new ConfigScopeBuilder().forCluster(clusterName).build();
    new ConfigAccessor(_gZkClient).set(scope, "healthChange" + ".enabled", "" + true);
  }

  protected void enablePersistBestPossibleAssignment(ZkClient zkClient, String clusterName,
      Boolean enable) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    HelixConfigScope clusterScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
            .forCluster(clusterName).build();

    configAccessor.set(clusterScope,
        ClusterConfig.ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.name(),
        enable.toString());
  }

  protected void disableDelayRebalanceInCluster(ZkClient zkClient, String clusterName,
      Boolean disabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    HelixConfigScope clusterScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
            .forCluster(clusterName).build();

    configAccessor
        .set(clusterScope, ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_DISABLED.name(),
            disabled.toString());
  }
}
