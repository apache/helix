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
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.LiveInstance.LiveInstanceProperty;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.util.HelixUtil;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class MockController {
  private final ZkClient client;
  private final String srcName;
  private final String clusterName;

  public MockController(String src, String zkServer, String cluster) {
    srcName = src;
    clusterName = cluster;
    client = new ZkClient(zkServer);
    client.setZkSerializer(new ZNRecordSerializer());
  }

  void sendMessage(MessageId msgId, String instanceName, String fromState, String toState,
      String partitionKey, int partitionId) throws InterruptedException, JsonGenerationException,
      JsonMappingException, IOException {
    Message message = new Message(MessageType.STATE_TRANSITION, msgId);
    message.setMessageId(msgId);
    message.setSrcName(srcName);
    message.setTgtName(instanceName);
    message.setMsgState(MessageState.NEW);
    message.setFromState(State.from(fromState));
    message.setToState(State.from(toState));
    // message.setPartitionId(partitionId);
    message.setPartitionId(PartitionId.from(partitionKey));

    String path = HelixUtil.getMessagePath(clusterName, instanceName) + "/" + message.getId();
    ObjectMapper mapper = new ObjectMapper();
    StringWriter sw = new StringWriter();
    mapper.writeValueUsingView(sw, message, Message.class);
    System.out.println(sw.toString());
    client.delete(path);

    Thread.sleep(10000);
    ZNRecord record = client.readData(HelixUtil.getLiveInstancePath(clusterName, instanceName));
    message.setTgtSessionId(SessionId.from(record.getSimpleField(
        LiveInstanceProperty.SESSION_ID.toString()).toString()));
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
