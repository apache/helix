package com.linkedin.clustermanager.mock.storage;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.mock.consumer.ConsumerAdapter;
import com.linkedin.clustermanager.mock.consumer.RelayConfig;
import com.linkedin.clustermanager.mock.consumer.RelayConsumer;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.statemachine.StateModel;

public class StorageStateModel extends StateModel
{

    // private Map<Integer, RelayConsumer> relayConsumersMap;
    private RelayConsumer consumer = null;
    private RelayConfig relayConfig;
    private StorageAdapter storage;

    private static Logger logger = Logger.getLogger(StorageStateModel.class);

    public StorageStateModel(String stateUnitKey, StorageAdapter storageAdapter)
    {
        // relayConsumersMap = new HashMap<Integer,RelayConsumer>();
        storage = storageAdapter;
        // this.consumerAdapter = consumerAdapter;
    }

    public RelayConfig getRelayConfig()
    {
        return relayConfig;
    }

    public void setRelayConfig(RelayConfig relayConfig)
    {
        this.relayConfig = relayConfig;
    }

    void checkDebug(Message task) throws Exception
    {
        // For debugging purposes
        if ((Boolean) task.getDebug() == true)
        {
            throw new Exception("Exception for debug");
        }
    }

    // @transition(to='to',from='from',blah blah..)
    public void onBecomeSlaveFromOffline(Message task,
            NotificationContext context) throws Exception
    {

        logger.info("Becoming slave from offline");

        checkDebug(task);

        String partition = (String) task.getStateUnitKey();
        String[] pdata = partition.split("\\.");
        String dbName = pdata[0];

        // Initializations for the storage node to create right tables, indexes
        // etc.
        storage.init(partition);
        storage.setPermissions(partition, "READONLY");

        // start consuming from the relay
        consumer = storage.getNewRelayConsumer(dbName, partition);
        consumer.start();
        // TODO: how do we know we are caught up?

        logger.info("Became slave for partition " + partition);
    }

    // @transition(to='to',from='from',blah blah..)
    public void onBecomeSlaveFromMaster(Message task,
            NotificationContext context) throws Exception
    {

        logger.info("Becoming slave from master");

        checkDebug(task);

        String partition = (String) task.getStateUnitKey();
        String[] pdata = partition.split("\\.");
        String dbName = pdata[0];
        storage.setPermissions(partition, "READONLY");
        storage.waitForWrites(partition);

        // start consuming from the relay
        consumer = storage.getNewRelayConsumer(dbName, partition);
        consumer.start();

        logger.info("Becamse slave for partition " + partition);
    }

    // @transition(to='to',from='from',blah blah..)
    public void onBecomeMasterFromSlave(Message task,
            NotificationContext context) throws Exception
    {
        logger.info("Becoming master from slave");

        checkDebug(task);

        String partition = (String) task.getStateUnitKey();

        // stop consumer and refetch from all so all changes are drained
        consumer.flush(); // blocking call

        // TODO: publish the hwm somewhere
        long hwm = consumer.getHwm();
        storage.setHwm(partition, hwm);
        storage.removeConsumer(partition);
        consumer = null;

        // set generation in storage
        Integer generationId = (Integer) task.getGeneration();
        storage.setGeneration(partition, generationId);

        storage.setPermissions(partition, "READWRITE");

        logger.info("Became master for partition " + partition);
    }

    // @transition(to='to',from='from',blah blah..)
    public void onBecomeOfflineFromSlave(Message task,
            NotificationContext context) throws Exception
    {

        logger.info("Becoming offline from slave");

        checkDebug(task);

        String partition = (String) task.getStateUnitKey();

        consumer.stop();
        storage.removeConsumer(partition);
        consumer = null;

        storage.setPermissions(partition, "OFFLINE");

        logger.info("Became offline for partition " + partition);
    }
}
