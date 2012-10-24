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
package org.apache.helix.mock.consumer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.DataAccessor;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.log4j.Logger;


public class ConsumerAdapter implements ExternalViewChangeListener
{

  HelixManager relayHelixManager;
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
    relayHelixManager = HelixManagerFactory.getZKHelixManager(clusterName,
                                                                    null,
                                                                    InstanceType.SPECTATOR,
                                                                    zkServer);

    relayHelixManager.connect();
    relayHelixManager.addExternalViewChangeListener(this);

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
