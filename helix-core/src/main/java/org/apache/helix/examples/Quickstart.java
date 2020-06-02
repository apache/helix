package org.apache.helix.examples;

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

import java.io.File;
import java.util.*;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.SemiAutoModeISBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.zookeeper.zkclient.IDefaultNameSpace;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.helix.zookeeper.zkclient.ZkServer;


public class Quickstart {

  private static String ZK_ADDRESS = "localhost:2199";
  private static String CLUSTER_NAME = "HELIX_QUICKSTART";
  private static int NUM_NODES = 2;
  private static final String RESOURCE_NAME = "MyResource";
  private static final int NUM_PARTITIONS = 8;
  private static final int NUM_REPLICAS = 1;

  private static final String STATE_MODEL_NAME = "MyStateModel";

  // states
  private static final String SLAVE = "SLAVE";
  private static final String OFFLINE = "OFFLINE";
  private static final String MASTER = "MASTER";
  private static final String DROPPED = "DROPPED";

  private static List<InstanceConfig> INSTANCE_CONFIG_LIST;
  private static List<MyProcess> PROCESS_LIST;
  private static HelixAdmin admin;
  static {
    INSTANCE_CONFIG_LIST = new ArrayList<InstanceConfig>();
    PROCESS_LIST = new ArrayList<Quickstart.MyProcess>();
    for (int i = 0; i < NUM_NODES; i++) {
      int port = 12000 + i;
      InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort("" + port);
      instanceConfig.setInstanceEnabled(true);
      INSTANCE_CONFIG_LIST.add(instanceConfig);
    }
  }

  public static void setup() {
    admin = new ZKHelixAdmin(ZK_ADDRESS);
    // create cluster
    echo("Creating cluster: " + CLUSTER_NAME);
    admin.addCluster(CLUSTER_NAME, true);

    // Add nodes to the cluster
    echo("Adding " + NUM_NODES + " participants to the cluster");
    for (int i = 0; i < NUM_NODES; i++) {
      admin.addInstance(CLUSTER_NAME, INSTANCE_CONFIG_LIST.get(i));
      echo("\t Added participant: " + INSTANCE_CONFIG_LIST.get(i).getInstanceName());
    }

    // Add a state model
    StateModelDefinition myStateModel = defineStateModel();
    echo("Configuring StateModel: " + "MyStateModel  with 1 Master and 1 Slave");
    admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL_NAME, myStateModel);

    // Add a resource with 6 partitions and 2 replicas
    echo("Adding a resource MyResource: " + "with 6 partitions and 2 replicas");
//    admin.addResource(CLUSTER_NAME, RESOURCE_NAME, NUM_PARTITIONS, STATE_MODEL_NAME, "AUTO");
    admin.addResource(CLUSTER_NAME, RESOURCE_NAME, buildCustomIdealStateFor(RESOURCE_NAME, NUM_PARTITIONS, NUM_NODES));
    // this will set up the ideal state, it calculates the preference list for
    // each partition similar to consistent hashing
//    admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, NUM_REPLICAS);
  }

  public static IdealState buildCustomIdealStateFor(String topicName,
                                                    int numTopicPartitions,
                                                    int numNodes) {

    final SemiAutoModeISBuilder customModeIdealStateBuilder = new SemiAutoModeISBuilder(topicName);

    customModeIdealStateBuilder
            .setStateModel(STATE_MODEL_NAME)
            .setNumPartitions(numTopicPartitions)
            .setNumReplica(1)
            .setMaxPartitionsPerNode(numTopicPartitions);

    for (int i = 0; i < numTopicPartitions; ++i) {
      customModeIdealStateBuilder.assignPreferenceList(
        Integer.toString(i),
        INSTANCE_CONFIG_LIST.get(i % numNodes).getInstanceName()
      );
    }
    return customModeIdealStateBuilder.build();
  }

  private static StateModelDefinition defineStateModel() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);
    // Add states and their rank to indicate priority. Lower the rank higher the
    // priority
    builder.addState(MASTER, 1);
//    builder.addState(SLAVE, 2);
    builder.addState(OFFLINE, 2);
    builder.addState(DROPPED);
    // Set the initial state when the node starts
    builder.initialState(OFFLINE);

    // Add transitions between the states.
    builder.addTransition(MASTER, OFFLINE);
    builder.addTransition(OFFLINE, MASTER);
    builder.addTransition(OFFLINE, DROPPED);

    // set constraints on states.
    // static constraint
    builder.upperBound(MASTER, 1);
    // dynamic constraint, R means it should be derived based on the replication
    // factor.
//    builder.dynamicUpperBound(SLAVE, "R");

    StateModelDefinition statemodelDefinition = builder.build();
    return statemodelDefinition;
  }

  public static void startController() {
    // start controller
    echo("Starting Helix Controller");
    HelixControllerMain.startHelixController(ZK_ADDRESS, CLUSTER_NAME, "localhost_9100",
        HelixControllerMain.STANDALONE);
  }

  public static void startNodes() throws Exception {
    echo("Starting Participants");
    for (int i = 0; i < NUM_NODES; i++) {
      MyProcess process = new MyProcess(INSTANCE_CONFIG_LIST.get(i).getId());
      PROCESS_LIST.add(process);
      process.start();
      echo("\t Started Participant: " + INSTANCE_CONFIG_LIST.get(i).getId());
    }
  }

  public static void startZookeeper() {
    echo("STARTING Zookeeper at " + ZK_ADDRESS);
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient) {
      }
    };
    new File("/tmp/helix-quickstart").mkdirs();
    // start zookeeper
    ZkServer server =
        new ZkServer("/tmp/helix-quickstart/dataDir", "/tmp/helix-quickstart/logDir",
            defaultNameSpace, 2199);
    server.start();
  }

  public static void echo(Object obj) {
    System.out.println(obj);
  }

  private static void waitForInput() {
    Scanner scanner = new Scanner(System.in);
    System.out.println("Waiting for user input...");
    while(!scanner.hasNextLine()) {
    }
    String line = scanner.nextLine();
    if (line.equals("exit")) {
      System.exit(0);
    }
//    scanner.close();
  }

  public static void main(String[] args) throws Exception {
    startZookeeper();
    setup();
    startNodes();
    startController();
    Thread.sleep(8000);
    printState("After starting 2 nodes");
//    waitForInput();
    addNode();
    Thread.sleep(15000);
    printState("After adding a third node");
//    waitForInput();
//    printState("");
    stopNode();
    Thread.sleep(14000);
    printState("After the 3rd node stops/crashes");
//    Thread.currentThread().join();
//    System.exit(0);

    while(true) {
      waitForInput();
      printState("");
    }
  }

  private static void addNode() throws Exception {

    NUM_NODES = NUM_NODES + 1;
    int port = 12000 + NUM_NODES - 1;
    InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort("" + port);
    instanceConfig.setInstanceEnabled(true);
    echo("ADDING NEW NODE :" + instanceConfig.getInstanceName()
        + ". Partitions will move from old nodes to the new node.");
    admin.addInstance(CLUSTER_NAME, instanceConfig);
    INSTANCE_CONFIG_LIST.add(instanceConfig);
    MyProcess process = new MyProcess(instanceConfig.getInstanceName());
    PROCESS_LIST.add(process);
//    admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, 1);
    admin.setResourceIdealState(
      CLUSTER_NAME, RESOURCE_NAME, buildCustomIdealStateFor(RESOURCE_NAME, NUM_PARTITIONS, NUM_NODES)
    );
    process.start();
  }

  private static void stopNode() {
    int nodeId = NUM_NODES - 1;
    NUM_NODES -= 1;
    echo("STOPPING " + INSTANCE_CONFIG_LIST.get(nodeId).getInstanceName()
        + ". Mastership will be transferred to the remaining nodes");
    PROCESS_LIST.get(nodeId).stop();
    PROCESS_LIST.remove(PROCESS_LIST.size() - 1);
    admin.dropInstance(CLUSTER_NAME, INSTANCE_CONFIG_LIST.get(nodeId));
    INSTANCE_CONFIG_LIST.remove(INSTANCE_CONFIG_LIST.size() - 1);
    admin.setResourceIdealState(
      CLUSTER_NAME, RESOURCE_NAME, buildCustomIdealStateFor(RESOURCE_NAME, NUM_PARTITIONS, NUM_NODES)
    );
  }

  private static void printState(String msg) {
    System.out.println("CLUSTER STATE: " + msg);
    ExternalView resourceExternalView = admin.getResourceExternalView(CLUSTER_NAME, RESOURCE_NAME);
    TreeSet<String> sortedSet = new TreeSet<String>(resourceExternalView.getPartitionSet());
    StringBuilder sb = new StringBuilder("\t\t");
    for (int i = 0; i < NUM_NODES; i++) {
      sb.append(INSTANCE_CONFIG_LIST.get(i).getInstanceName()).append("\t");
    }
    System.out.println(sb);
    for (String partitionName : sortedSet) {
      sb.delete(0, sb.length() - 1);
      sb.append(partitionName).append("\t");
      for (int i = 0; i < NUM_NODES; i++) {
        Map<String, String> stateMap = resourceExternalView.getStateMap(partitionName);
        if (stateMap != null && stateMap.containsKey(INSTANCE_CONFIG_LIST.get(i).getInstanceName())) {
          sb.append(stateMap.get(INSTANCE_CONFIG_LIST.get(i).getInstanceName()).charAt(0)).append(
              "\t\t");
        } else {
          sb.append("-").append("\t\t");
        }
      }
      System.out.println(sb);
    }
    System.out.println("###################################################################");
  }

  static final class MyProcess {
    private final String instanceName;
    private HelixManager manager;

    public MyProcess(String instanceName) {
      this.instanceName = instanceName;
    }

    public void start() throws Exception {
      manager =
          HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, instanceName,
              InstanceType.PARTICIPANT, ZK_ADDRESS);

      MasterSlaveStateModelFactory stateModelFactory =
          new MasterSlaveStateModelFactory(instanceName);

      StateMachineEngine stateMach = manager.getStateMachineEngine();
      stateMach.registerStateModelFactory(STATE_MODEL_NAME, stateModelFactory);
      manager.connect();

    }

    public void stop() {
      manager.disconnect();
    }
  }

}
