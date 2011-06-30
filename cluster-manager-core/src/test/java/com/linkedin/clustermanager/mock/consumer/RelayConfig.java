package com.linkedin.clustermanager.mock.consumer;

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
