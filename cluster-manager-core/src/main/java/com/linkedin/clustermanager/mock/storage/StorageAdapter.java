package com.linkedin.clustermanager.mock.storage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.core.*;
import com.linkedin.clustermanager.core.listeners.ExternalViewChangeListener;
import com.linkedin.clustermanager.core.listeners.MessageListener;
import com.linkedin.clustermanager.mock.consumer.ConsumerAdapter;
import com.linkedin.clustermanager.mock.consumer.RelayConfig;
import com.linkedin.clustermanager.mock.consumer.RelayConsumer;
import com.linkedin.clustermanager.statemachine.StateMachineEngine;

class StorageAdapter
{
    ClusterManager relayClusterManager;
    ClusterManager storageClusterManager;

    ClusterDataAccessor relayClusterClient;
    ClusterDataAccessor storageClusterClient;

    private ExternalViewChangeListener relayViewHolder;
    private MessageListener genericStateMachineHandler;

    // Map<Object, RelayConsumer> relayConsumersMap;
    private ConsumerAdapter consumerAdapter;
    private StorageStateModelFactory stateModelFactory;

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

        storageClusterManager = ClusterManagerFactory
                .getZKBasedManagerForParticipant(clusterName, instanceName,
                        zkConnectString);
        stateModelFactory = new StorageStateModelFactory(this);
        genericStateMachineHandler = new StateMachineEngine(stateModelFactory);
        storageClusterManager.addMessageListener(genericStateMachineHandler,
                instanceName);
        storageClusterClient = storageClusterManager.getDataAccessor();

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
     * During replication set up which will happen when there is state
     * transition //TODO may not be nee
     */
    void getMasteredPartitions()
    {

    }

    /*
     * During replication set up which will happen when there is state
     * transition
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
        }
        else
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
