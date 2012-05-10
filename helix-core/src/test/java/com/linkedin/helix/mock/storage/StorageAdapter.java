/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.mock.storage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.mock.consumer.ConsumerAdapter;
import com.linkedin.helix.mock.consumer.RelayConfig;
import com.linkedin.helix.mock.consumer.RelayConsumer;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.participant.StateMachineEngine;

class StorageAdapter
{
  HelixManager relayHelixManager;
  HelixManager storageHelixManager;

  DataAccessor relayClusterClient;
  DataAccessor storageClusterClient;

  private ExternalViewChangeListener relayViewHolder;
  private MessageListener messageListener;

  // Map<Object, RelayConsumer> relayConsumersMap;
  private final ConsumerAdapter consumerAdapter;
  private final StorageStateModelFactory stateModelFactory;

  class partitionData
  {
    long initTime;
    String permissions;
    int generationId;
    long hwm;
  }

  Map<String, partitionData> hostedPartitions;
  private final String instanceName;

  private static Logger logger = Logger.getLogger(StorageAdapter.class);

  public StorageAdapter(String instanceName, String zkConnectString,
      String clusterName, String relayClusterName) throws Exception
  {

    this.instanceName = instanceName;

    hostedPartitions = new ConcurrentHashMap<String, partitionData>();

    storageHelixManager = HelixManagerFactory
        .getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
                             zkConnectString);
    stateModelFactory = new StorageStateModelFactory(this);
//    StateMachineEngine genericStateMachineHandler = new StateMachineEngine();
    StateMachineEngine stateMach = storageHelixManager.getStateMachineEngine();
    stateMach.registerStateModelFactory("MasterSlave", stateModelFactory);

    storageHelixManager.getMessagingService()
      .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(), stateMach);
    storageHelixManager.connect();
    storageClusterClient = storageHelixManager.getDataAccessor();

    consumerAdapter = new ConsumerAdapter(instanceName, zkConnectString,
        relayClusterName);
  }

  // for every write call
  public boolean isMasterForPartition(Integer partitionId)
  {
    StorageStateModel stateModelForParition = stateModelFactory
        .getStateModelForPartition(partitionId);
    return "MASTER".equals(stateModelForParition.getCurrentState());
  }

  // for every read call depending on read scale config
  public boolean isReplicaForPartition(Integer partitionId)
  {
    StorageStateModel stateModelForParition = stateModelFactory
        .getStateModelForPartition(partitionId);
    return "REPLICA".equals(stateModelForParition.getCurrentState());
  }

  /**
   * During replication set up which will happen when there is state transition
   * //TODO may not be nee
   */
  void getMasteredPartitions()
  {

  }

  /*
   * During replication set up which will happen when there is state transition
   *
   * @return
   */
  Map<Integer, RelayConfig> getReplicatedPartitions()
  {
    return null;
  }

  /**
   * Will be used in relay consumers, return can be one RelayConfig or List
   * depending on implementation
   */
  List<RelayConfig> getRelaysForPartition(Integer partitionId)
  {
    return null;
  }

  void updateHighWaterMarkForPartition(String waterMark, Integer partitionId)
  {

  }

  public void endProcess()
  {

  }

  public void start()
  {
    logger.info("Started storage node " + instanceName);
  }

  public void setGeneration(String partition, Integer generationId)
  {
    partitionData pdata = hostedPartitions.get(partition);
    pdata.generationId = generationId;
    hostedPartitions.put(partition, pdata);
  }

  public void setHwm(String partition, long hwm)
  {
    partitionData pdata = hostedPartitions.get(partition);
    pdata.hwm = hwm;
    hostedPartitions.put(partition, pdata);
  }

  // TODO: make sure multiple invocations are possible
  public void init(String partition)
  {
    logger.info("Storage initializing partition " + partition);
    if (hostedPartitions.containsKey(partition))
    {
      logger.info("Partition exists, not reinitializing.");
    } else
    {
      partitionData pdata = new partitionData();
      pdata.initTime = System.currentTimeMillis();
      pdata.permissions = "OFFLINE";
      hostedPartitions.put(partition, pdata);
    }
    logger.info("Storage initialized for partition " + partition);
  }

  public void setPermissions(String partition, String permissions)
  {
    partitionData pdata = hostedPartitions.get(partition);
    pdata.permissions = permissions;
    hostedPartitions.put(partition, pdata);
  }

  public void waitForWrites(String partition)
  {
    // TODO Auto-generated method stub

  }

  public RelayConsumer getNewRelayConsumer(String dbName, String partition)
      throws Exception
  {
    logger.info("Got new relayconsumer for " + partition);
    return consumerAdapter.getNewRelayConsumer(dbName, partition);
  }

  public void removeConsumer(String partition) throws Exception
  {
    logger.info("Removing consumer for partition " + partition);
    consumerAdapter.removeConsumer(partition);

  }

}
