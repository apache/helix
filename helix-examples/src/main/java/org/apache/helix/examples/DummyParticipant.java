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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

public class DummyParticipant {
  // dummy master-slave state model
  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "ERROR"
  })
  public static class DummyMSStateModel extends TransitionHandler {
    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      PartitionId partitionId = message.getPartitionId();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes SLAVE from OFFLINE for " + partitionId);
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      PartitionId partitionId = message.getPartitionId();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes MASTER from SLAVE for " + partitionId);
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      PartitionId partitionId = message.getPartitionId();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes SLAVE from MASTER for " + partitionId);
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      PartitionId partitionId = message.getPartitionId();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes OFFLINE from SLAVE for " + partitionId);
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      PartitionId partitionId = message.getPartitionId();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes DROPPED from OFFLINE for " + partitionId);
    }

    @Transition(to = "OFFLINE", from = "ERROR")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      PartitionId partitionId = message.getPartitionId();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes OFFLINE from ERROR for " + partitionId);
    }

    @Override
    public void reset() {
      System.out.println("Default MockMSStateModel.reset() invoked");
    }
  }

  // dummy master slave state model factory
  public static class DummyMSModelFactory extends StateTransitionHandlerFactory<DummyMSStateModel> {
    @Override
    public DummyMSStateModel createStateTransitionHandler(ResourceId resource, PartitionId partitionName) {
      DummyMSStateModel model = new DummyMSStateModel();
      return model;
    }
  }

  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("USAGE: DummyParticipant zkAddress clusterName instanceName");
      System.exit(1);
    }

    String zkAddr = args[0];
    String clusterName = args[1];
    String instanceName = args[2];

    HelixManager manager = null;
    try {
      manager =
          HelixManagerFactory.getZKHelixManager(clusterName, instanceName,
              InstanceType.PARTICIPANT, zkAddr);

      StateMachineEngine stateMach = manager.getStateMachineEngine();
      DummyMSModelFactory msModelFactory = new DummyMSModelFactory();
      stateMach.registerStateModelFactory(StateModelDefId.MasterSlave, msModelFactory);

      manager.connect();

      Thread.currentThread().join();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (manager != null) {
        manager.disconnect();
      }
    }
  }
}
