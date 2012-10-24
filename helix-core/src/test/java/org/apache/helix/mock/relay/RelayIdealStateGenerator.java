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
package org.apache.helix.mock.relay;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.IdealState.IdealStateProperty;


public class RelayIdealStateGenerator
{
  public static void main(String[] args)
  {
    ZNRecord record = new ZNRecord("SdrRelay");
    record.setSimpleField(IdealStateProperty.NUM_PARTITIONS.toString(), "28");
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
