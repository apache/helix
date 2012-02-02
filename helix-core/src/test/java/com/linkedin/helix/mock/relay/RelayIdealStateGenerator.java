package com.linkedin.helix.mock.relay;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.model.IdealState.IdealStateProperty;

public class RelayIdealStateGenerator
{
  public static void main(String[] args)
  {
    ZNRecord record = new ZNRecord("SdrRelay");
    record.setSimpleField(IdealStateProperty.PARTITIONS.toString(), "28");
    for (int i = 22; i < 28; i++)
    {
      String key = "ela4-db-sdr.prod.linkedin.com_1521,sdr1,sdr_people_search_,p"
          + i + ",MASTER";
      Map<String, String> map = new HashMap<String, String>();
      for (int j = 0; j < 4; j++)
      {
        String instanceName = "ela4-rly0" + j + ".prod.linkedin.com_10015";
        map.put(instanceName, "ONLINE");
      }
      record.getMapFields().put(key, map);
    }

    ZNRecordSerializer serializer = new ZNRecordSerializer();
    System.out.println(new String(serializer.serialize(record)));
  }
}
