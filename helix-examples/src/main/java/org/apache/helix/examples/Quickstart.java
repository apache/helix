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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.State;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.StateMachineEngine;

public class Quickstart {

  private static String ZK_ADDRESS = "localhost:2199";
  private static String CLUSTER_NAME = "HELIX_QUICKSTART";
  private static int NUM_NODES = 2;
  private static final String RESOURCE_NAME = "MyResource";
  private static final int NUM_PARTITIONS = 6;
  private static final int NUM_REPLICAS = 2;

  private static final StateModelDefId STATE_MODEL_NAME = StateModelDefId.from("MyStateModel");

  // states
  private static final State SLAVE = State.from("SLAVE");
  private static final State OFFLINE = State.from("OFFLINE");
  private static final State MASTER = State.from("MASTER");
  private static final State DROPPED = State.from("DROPPED");

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
    admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL_NAME.stringify(), myStateModel);

    // Add a resource with 6 partitions and 2 replicas
    echo("Adding a resource MyResource: " + "with 6 partitions and 2 replicas");
    admin.addResource(CLUSTER_NAME, RESOURCE_NAME, NUM_PARTITIONS, STATE_MODEL_NAME.stringify(),
        "AUTO");
    // this will set up the ideal state, it calculates the preference list for
    // each partition similar to consistent hashing
    admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, NUM_REPLICAS);
  }

  private static StateModelDefinition defineStateModel() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);
    // Add states and their rank to indicate priority. Lower the rank higher the
    // priority
    builder.addState(MASTER, 1);
    builder.addState(SLAVE, 2);
    builder.addState(OFFLINE);
    builder.addState(DROPPED);
    // Set the initial state when the node starts
    builder.initialState(OFFLINE);

    // Add transitions between the states.
    builder.addTransition(OFFLINE, SLAVE);
    builder.addTransition(SLAVE, OFFLINE);
    builder.addTransition(SLAVE, MASTER);
    builder.addTransition(MASTER, SLAVE);
    builder.addTransition(OFFLINE, DROPPED);

    // set constraints on states.
    // static constraint
    builder.upperBound(MASTER, 1);
    // dynamic constraint, R means it should be derived based on the replication
    // factor.
    builder.dynamicUpperBound(SLAVE, "R");

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

  public static void main(String[] args) throws Exception {
    startZookeeper();
    setup();
    startNodes();
    startController();
    Thread.sleep(5000);
    printState("After starting 2 nodes");
    addNode();
    Thread.sleep(5000);
    printState("After adding a third node");
    stopNode();
    Thread.sleep(5000);
    printState("After the 3rd node stops/crashes");
    Thread.currentThread().join();
    System.exit(0);
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
    admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, 3);
    process.start();
  }

  private static void stopNode() {
    int nodeId = NUM_NODES - 1;
    echo("STOPPING " + INSTANCE_CONFIG_LIST.get(nodeId).getInstanceName()
        + ". Mastership will be transferred to the remaining nodes");
    PROCESS_LIST.get(nodeId).stop();
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
