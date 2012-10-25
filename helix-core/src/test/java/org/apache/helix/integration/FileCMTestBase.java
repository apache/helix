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

import java.util.Date;
import java.util.List;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.manager.file.FileHelixAdmin;
import org.apache.helix.mock.storage.DummyProcess;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
import org.apache.helix.store.PropertyJsonComparator;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.store.file.FilePropertyStore;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.IdealStateCalculatorForStorageNode;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


/**
 * Test base for dynamic file-based cluster manager
 * 
 * @author zzhang
 * 
 */

public class FileCMTestBase
{
  private static Logger                       logger       =
                                                               Logger.getLogger(FileCMTestBase.class);
  protected final String                      CLUSTER_NAME = "CLUSTER_"
                                                               + getShortClassName();
  private static final String                 TEST_DB      = "TestDB";
  protected static final String               STATE_MODEL  = "MasterSlave";
  protected static final int                  NODE_NR      = 5;
  protected static final int                  START_PORT   = 12918;
  final String                                ROOT_PATH    = "/tmp/"
                                                               + getShortClassName();

  protected final FilePropertyStore<ZNRecord> _fileStore   =
                                                               new FilePropertyStore<ZNRecord>(new PropertyJsonSerializer<ZNRecord>(ZNRecord.class),
                                                                                               ROOT_PATH,
                                                                                               new PropertyJsonComparator<ZNRecord>(ZNRecord.class));
  protected HelixManager                      _manager;
  protected HelixAdmin                        _mgmtTool;

  @BeforeClass()
  public void beforeClass() throws Exception
  {
    System.out.println("START BEFORECLASS FileCMTestBase at "
        + new Date(System.currentTimeMillis()));

    // setup test cluster
    _mgmtTool = new FileHelixAdmin(_fileStore);
    _mgmtTool.addCluster(CLUSTER_NAME, true);
    
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    _mgmtTool.addStateModelDef(CLUSTER_NAME, "LeaderStandby",
                     new StateModelDefinition(generator.generateConfigForLeaderStandby()));

    _mgmtTool.addStateModelDef(CLUSTER_NAME,
                               "OnlineOffline",
                               new StateModelDefinition(generator.generateConfigForOnlineOffline()));
    _mgmtTool.addResource(CLUSTER_NAME, TEST_DB, 10, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++)
    {
      addNodeToCluster(CLUSTER_NAME, "localhost", START_PORT + i);
    }
    rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);

    // start dummy storage nodes
    for (int i = 0; i < NODE_NR; i++)
    {
      DummyProcess process =
          new DummyProcess(null,
                           CLUSTER_NAME,
                           "localhost_" + (START_PORT + i),
                           "dynamic-file",
                           null,
                           0,
                           _fileStore);
      try
      {
        process.start();
      }
      catch (Exception e)
      {
        logger.error("fail to start dummy participant using dynmaic file-based cluster-manager",
                     e);
      }

      _manager =
          HelixManagerFactory.getDynamicFileHelixManager(CLUSTER_NAME,
                                                         "controller_0",
                                                         InstanceType.CONTROLLER,
                                                         _fileStore);

    }

    // start cluster manager controller
    GenericHelixController controller = new GenericHelixController();
    try
    {
      // manager.addConfigChangeListener(controller);
      _manager.addLiveInstanceChangeListener(controller);
      _manager.addIdealStateChangeListener(controller);
      // manager.addExternalViewChangeListener(controller);
      _manager.connect();
    }
    catch (Exception e)
    {
      logger.error("fail to start controller using dynamic file-based cluster-manager ",
                   e);
    }

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewFileVerifier(ROOT_PATH,
                                                                                                     CLUSTER_NAME));
    Assert.assertTrue(result);

    System.out.println("END BEFORECLASS FileCMTestBase at "
        + new Date(System.currentTimeMillis()));
  }

  @AfterClass()
  public void afterClass() throws Exception
  {
    logger.info("START afterClass FileCMTestBase shutting down file-based cluster managers at "
        + new Date(System.currentTimeMillis()));

    // Thread.sleep(3000);
    // _store.stop();
    _manager.disconnect();
    _manager.disconnect(); // test if disconnect() can be called twice

    logger.info("END afterClass FileCMTestBase at "
        + new Date(System.currentTimeMillis()));

  }

  private String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }

  private void addNodeToCluster(String clusterName, String host, int port)
  {
    // TODO use ClusterSetup
    String nodeId = host + "_" + port;
    ZNRecord nodeConfig = new ZNRecord(nodeId);
    nodeConfig.setSimpleField(InstanceConfigProperty.HELIX_HOST.toString(), host);
    nodeConfig.setSimpleField(InstanceConfigProperty.HELIX_PORT.toString(),
                              Integer.toString(port));
    nodeConfig.setSimpleField(InstanceConfigProperty.HELIX_ENABLED.toString(),
                              Boolean.toString(true));
    _mgmtTool.addInstance(CLUSTER_NAME, new InstanceConfig(nodeConfig));
  }

  protected void rebalanceStorageCluster(String clusterName,
                                         String resourceName,
                                         int replica)
  {
    List<String> nodeNames = _mgmtTool.getInstancesInCluster(clusterName);

    IdealState idealState = _mgmtTool.getResourceIdealState(clusterName, resourceName);
    idealState.setReplicas(Integer.toString(replica));
    int partitions = idealState.getNumPartitions();

    ZNRecord newIdealState =
        IdealStateCalculatorForStorageNode.calculateIdealState(nodeNames,
                                                               partitions,
                                                               replica - 1,
                                                               resourceName,
                                                               "MASTER",
                                                               "SLAVE");

    newIdealState.merge(idealState.getRecord());
    _mgmtTool.setResourceIdealState(clusterName,
                                    resourceName,
                                    new IdealState(newIdealState));
  }
}
