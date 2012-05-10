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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

public class RelayConfig
{
  String partitionId;
  private ArrayList<String> masterRelays;
  private ArrayList<String> slaveRelays;

  public RelayConfig(Map<String, String> relayList)
  {

    masterRelays = new ArrayList<String>();
    slaveRelays = new ArrayList<String>();

    for (Entry<String, String> entry : relayList.entrySet())
    {
      String relayInstance = entry.getKey();
      String relayState = entry.getValue();

      if (relayState.equals("MASTER"))
      {
        masterRelays.add(relayInstance);
      } else if (relayState.equals("SLAVE"))
      {
        slaveRelays.add(relayInstance);
      }
    }
  }

  public String getClusterName()
  {
    // TODO Auto-generated method stub
    return null;
  }

  public String getzkServer()
  {
    // TODO Auto-generated method stub
    return null;
  }

  public String getMaster()
  {
    if (masterRelays.isEmpty())
      return null;

    return masterRelays.get(0);
  }

  public List<String> getRelays()
  {
    List<String> relays = new ArrayList<String>();
    relays.addAll(masterRelays);
    relays.addAll(slaveRelays);

    return relays;
  }

}
