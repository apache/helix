	package com.linkedin.helix.integration;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.helix.HelixAdmin;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.GenericHelixController;
import com.linkedin.helix.manager.file.FileHelixAdmin;
import com.linkedin.helix.mock.storage.DummyProcess;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.InstanceConfig.InstanceConfigProperty;
import com.linkedin.helix.store.PropertyJsonComparator;
import com.linkedin.helix.store.PropertyJsonSerializer;
import com.linkedin.helix.store.file.FilePropertyStore;
import com.linkedin.helix.tools.IdealStateCalculatorForStorageNode;

/**
 * Test base for dynamic file-based cluster manager
 *
 * @author zzhang
 *
 */

public class FileCMTestBase
{
  private static Logger logger = Logger.getLogger(FileCMTestBase.class);
  protected final String CLUSTER_NAME = "CLUSTER_" + getShortClassName();
  private static final String TEST_DB = "TestDB";
  protected static final String STATE_MODEL = "MasterSlave";
  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  private final String ROOT_PATH = "/tmp/" + getShortClassName();

  protected final FilePropertyStore<ZNRecord> _fileStore
    = new FilePropertyStore<ZNRecord>(new PropertyJsonSerializer<ZNRecord>(ZNRecord.class),
                                      ROOT_PATH,
                                      new PropertyJsonComparator<ZNRecord>(ZNRecord.class));
  protected HelixManager _manager;
  protected HelixAdmin _mgmtTool;

  @BeforeClass()
  public void beforeClass() throws Exception
  {
    System.out.println("START BEFORECLASS FileCMTestBase at " + new Date(System.currentTimeMillis()));

    // setup test cluster
    _mgmtTool = new FileHelixAdmin(_fileStore);
    _mgmtTool.addCluster(CLUSTER_NAME, true);
    _mgmtTool.addResource(CLUSTER_NAME, TEST_DB, 10, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++)
    {
      addNodeToCluster(CLUSTER_NAME, "localhost", START_PORT + i);
    }
    rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 2);

    // start dummy storage nodes
    for (int i = 0; i < NODE_NR; i++)
    {
      DummyProcess process = new DummyProcess(null,
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
        logger
            .error("fail to start dummy participant using dynmaic file-based cluster-manager", e);
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
    } catch (Exception e)
    {
      logger.error("fail to start controller using dynamic file-based cluster-manager ", e);
    }

    verifyCluster();
    
    System.out.println("END BEFORECLASS FileCMTestBase at " + new Date(System.currentTimeMillis()));
  }

  @AfterClass()
  public void afterClass() throws Exception
  {
    logger.info("START afterClass FileCMTestBase shutting down file-based cluster managers at "
        + new Date(System.currentTimeMillis()));

    // Thread.sleep(3000);
    // _store.stop();
    _manager.disconnect();
    _manager.disconnect();  // test if disconnect() can be called twice

    logger.info("END afterClass FileCMTestBase at " + new Date(System.currentTimeMillis()));

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
      String resourceName, int replica)
  {
    List<String> nodeNames = _mgmtTool.getInstancesInCluster(clusterName);

    IdealState idealState = _mgmtTool.getResourceIdealState(clusterName, resourceName);
    int partitions = idealState.getNumPartitions();

    ZNRecord newIdealState = IdealStateCalculatorForStorageNode
        .calculateIdealState(nodeNames, partitions, replica, resourceName,
            "MASTER", "SLAVE");

    newIdealState.merge(idealState.getRecord());
    _mgmtTool.setResourceIdealState(clusterName, resourceName, new IdealState(newIdealState));
  }

  protected void verifyCluster()
  {
    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewFile",
                                 30 * 1000,
                                 TEST_DB,
                                 10,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 _fileStore);
  }
}
