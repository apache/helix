package com.linkedin.helix.mock.consumer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixAgent;
import com.linkedin.helix.HelixAgentFactory;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.ExternalView;

public class ConsumerAdapter implements ExternalViewChangeListener
{

  HelixAgent relayClusterManager;
  DataAccessor relayClusterClient;
  private final ConcurrentHashMap<String, RelayConsumer> relayConsumers;
  private final ConcurrentHashMap<String, RelayConfig> relayConfigs;
  private static Logger logger = Logger.getLogger(ConsumerAdapter.class);

  public ConsumerAdapter(String instanceName, String zkServer,
      String clusterName) throws Exception
  {
    relayConsumers = new ConcurrentHashMap<String, RelayConsumer>();
    relayConfigs = new ConcurrentHashMap<String, RelayConfig>();

//    relayClusterManager = ClusterManagerFactory.getZKBasedManagerForSpectator(
//        clusterName, zkServer);
    relayClusterManager = HelixAgentFactory.getZKHelixAgent(clusterName,
                                                                    null,
                                                                    InstanceType.SPECTATOR,
                                                                    zkServer);

    relayClusterManager.connect();
    relayClusterManager.addExternalViewChangeListener(this);

  }

  private RelayConfig getRelayConfig(ZNRecord externalView, String partition)
  {
    LinkedHashMap<String, String> relayList = (LinkedHashMap<String, String>) externalView
        .getMapField(partition);

    if (relayList == null)
      return null;

    return new RelayConfig(relayList);

  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext)
  {
    logger.info("onExternalViewChange invoked");

    for (ExternalView subview : externalViewList)
    {
      Map<String, Map<String, String>> partitions = subview.getRecord().getMapFields();

      for (Entry<String, Map<String, String>> partitionConsumer : partitions
          .entrySet())
      {
        String partition = partitionConsumer.getKey();
        Map<String, String> relayList = partitionConsumer.getValue();
        RelayConfig relayConfig = new RelayConfig(relayList);
        relayConfigs.put(partition, relayConfig);
        RelayConsumer consumer = relayConsumers.get(partition);
        if (consumer != null)
        {
          consumer.setConfig(relayConfig);
        }
      }
    }
  }

  public void start()
  {
    // TODO Auto-generated method stub

  }

  public RelayConsumer getNewRelayConsumer(String dbName, String partition)
      throws Exception
  {
    RelayConsumer consumer = new RelayConsumer(null, partition);

    if (relayConsumers.putIfAbsent(partition, consumer) != null)
    {
      throw new Exception("Existing consumer");
    }
    logger.info("created new consumer for partition" + partition);

    RelayConfig relayConfig = relayConfigs.get(partition);
    if (relayConfig != null)
    {
      consumer.setConfig(relayConfig);
    }

    return consumer;
  }

  public void removeConsumer(String partition) throws Exception
  {
    if (relayConsumers.remove(partition) == null)
    {
      throw new Exception("Non Existing consumer for partition " + partition);
    }
    logger.info("Removed consumer for partition " + partition);
  }
}
