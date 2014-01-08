package org.apache.helix.provisioning.yarn;

import java.io.IOException;
import java.util.Map;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * This will <br/>
 * <ul>
 * <li>start zookeeper automatically</li>
 * <li>create the cluster</li>
 * <li>set up resource(s)</li>
 * <li>start helix controller</li>
 * </ul>
 */
public class HelixYarnApplicationMasterMain {
  public static void main(String[] args) throws Exception {
    // START ZOOKEEPER
    String dataDir = "dataDir";
    String logDir = "logDir";
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {

      @Override
      public void createDefaultNameSpace(ZkClient zkClient) {

      }
    };
    ZkServer server = new ZkServer(dataDir, logDir, defaultNameSpace);
    server.start();

   

    // start

    Map<String, String> envs = System.getenv();

    ContainerId containerId =
        ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
    ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
    
    //GenericApplicationMaster genAppMaster = new GenericApplicationMaster(appAttemptID);
    
    GenericApplicationMaster genericApplicationMaster = new GenericApplicationMaster(appAttemptID);
    genericApplicationMaster.start();
    
    YarnProvisioner.applicationMaster = genericApplicationMaster;
    
    // CREATE CLUSTER and setup the resources
    
    
  }
}
