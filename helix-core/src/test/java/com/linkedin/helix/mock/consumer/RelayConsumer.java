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
package com.linkedin.helix.mock.consumer;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;

public class RelayConsumer
{
  HelixManager relayHelixManager;
  DataAccessor relayClusterClient;
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
