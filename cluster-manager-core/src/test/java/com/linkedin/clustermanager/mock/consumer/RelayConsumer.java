package com.linkedin.clustermanager.mock.consumer;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;

public class RelayConsumer
{
    ClusterManager relayClusterManager;
    ClusterDataAccessor relayClusterClient;
    private final String partition;
    private RelayConfig currentRelay;
    private static Logger logger = Logger.getLogger(RelayConsumer.class);

    public RelayConsumer(RelayConfig relayConfig, String partition)
    {
        this.partition = partition;
        this.currentRelay = relayConfig;
    }

    public void stop()
    {
        if (currentRelay != null)
        {
            logger.info("RelayConsumer stopping listening from relay "
                    + currentRelay.getMaster());
        }
    }

    public boolean isPointingTo(RelayConfig relayConfig)
    {
        return false;
    }

    public void start()
    {
        if (currentRelay != null)
        {
            logger.info("RelayConsumer starting listening from relay "
                    + currentRelay.getMaster());
        }
    }

    /*
     * This is required at relayConsumer to reach out relays which are hosting
     * data for slaved partitions.
     */
    void getRelaysForPartition(Integer partitionId)
    {

    }

    public long getHwm()
    {
        // TODO this is supposed to return the last checkpoint from this
        // consumer
        return 0;
    }

    public String getPartition()
    {
        // TODO Auto-generated method stub
        return partition;
    }

    public Object getCurrentRelay()
    {
        // TODO Auto-generated method stub
        return currentRelay;
    }

    public synchronized void setConfig(RelayConfig relayConfig)
    {
        // TODO Auto-generated method stub
        currentRelay = relayConfig;
        logger.info("Setting config to relay " + relayConfig.getMaster());
    }

    public void flush()
    {
        assert (currentRelay != null);
        logger.info("Consumer flushing for partition " + partition);
        if (currentRelay == null || currentRelay.getRelays() == null)
            return;

        for (String relay : currentRelay.getRelays())
        {
            logger.info("Reading (flush) from relay " + relay);
        }

    }

}
