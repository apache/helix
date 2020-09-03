package org.apache.helix.mock.controller;

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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.LiveInstance.LiveInstanceProperty;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;


public class MockController {
  private final HelixZkClient client;
  private final String srcName;
  private final String clusterName;

  public MockController(String src, String zkServer, String cluster) {
    srcName = src;
    clusterName = cluster;
    client = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkServer));
    client.setZkSerializer(new ZNRecordSerializer());
  }

  void sendMessage(String msgId, String instanceName, String fromState, String toState,
      String partitionKey, int partitionId) throws InterruptedException, JsonGenerationException,
      JsonMappingException, IOException {
    Message message = new Message(MessageType.STATE_TRANSITION, msgId);
    message.setMsgId(msgId);
    message.setSrcName(srcName);
    message.setTgtName(instanceName);
    message.setMsgState(MessageState.NEW);
    message.setFromState(fromState);
    message.setToState(toState);
    // message.setPartitionId(partitionId);
    message.setPartitionName(partitionKey);

    String path = PropertyPathBuilder.instanceMessage(clusterName, instanceName, message.getId());
    ObjectMapper mapper = new ObjectMapper();
    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, message);
    System.out.println(sw.toString());
    client.delete(path);

    Thread.sleep(10000);
    ZNRecord record = client.readData(PropertyPathBuilder.liveInstance(clusterName, instanceName));
    message.setTgtSessionId(record.getSimpleField(LiveInstanceProperty.SESSION_ID.toString())
        .toString());
    client.createPersistent(path, message);
  }

  public void createExternalView(List<String> instanceNames, int partitions, int replicas,
      String dbName, long randomSeed) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(client));
    Builder keyBuilder = accessor.keyBuilder();

    ExternalView externalView =
        new ExternalView(computeRoutingTable(instanceNames, partitions, replicas, dbName,
            randomSeed));

    accessor.setProperty(keyBuilder.externalView(dbName), externalView);
  }

  public ZNRecord computeRoutingTable(List<String> instanceNames, int partitions, int replicas,
      String dbName, long randomSeed) {
    assert (instanceNames.size() > replicas);
    Collections.sort(instanceNames);

    ZNRecord result = new ZNRecord(dbName);

    Map<String, Object> externalView = new TreeMap<String, Object>();

    List<Integer> partitionList = new ArrayList<Integer>(partitions);
    for (int i = 0; i < partitions; i++) {
      partitionList.add(new Integer(i));
    }
    Random rand = new Random(randomSeed);
    // Shuffle the partition list
    Collections.shuffle(partitionList, rand);

    for (int i = 0; i < partitionList.size(); i++) {
      int partitionId = partitionList.get(i);
      Map<String, String> partitionAssignment = new TreeMap<String, String>();
      int masterNode = i % instanceNames.size();
      // the first in the list is the node that contains the master
      partitionAssignment.put(instanceNames.get(masterNode), "MASTER");

      // for the jth replica, we put it on (masterNode + j) % nodes-th
      // node
      for (int j = 1; j <= replicas; j++) {
        partitionAssignment
            .put(instanceNames.get((masterNode + j) % instanceNames.size()), "SLAVE");
      }
      String partitionName = dbName + ".partition-" + partitionId;
      result.setMapField(partitionName, partitionAssignment);
    }
    result.setSimpleField(IdealStateProperty.NUM_PARTITIONS.toString(), "" + partitions);
    return result;
  }
}
