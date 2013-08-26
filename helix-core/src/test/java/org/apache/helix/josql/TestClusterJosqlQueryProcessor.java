package org.apache.helix.josql;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.Criteria;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.strategy.DefaultTwoStateStrategy;
import org.apache.helix.model.LiveInstance.LiveInstanceProperty;
import org.josql.Query;
import org.josql.QueryExecutionException;
import org.josql.QueryParseException;
import org.josql.QueryResults;
import org.testng.annotations.Test;

public class TestClusterJosqlQueryProcessor {
  @Test(groups = {
    "unitTest"
  })
  public void queryClusterDataSample() {
    List<ZNRecord> liveInstances = new ArrayList<ZNRecord>();
    Map<String, ZNRecord> liveInstanceMap = new HashMap<String, ZNRecord>();
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < 5; i++) {
      String instance = "localhost_" + (12918 + i);
      instances.add(instance);
      ZNRecord metaData = new ZNRecord(instance);
      metaData.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(), UUID.randomUUID()
          .toString());
      metaData.setSimpleField("SCN", "" + (10 - i));
      liveInstances.add(metaData);
      liveInstanceMap.put(instance, metaData);
    }

    // liveInstances.remove(0);
    ZNRecord externalView =
        DefaultTwoStateStrategy.calculateIdealState(instances, 21, 3, "TestDB", "MASTER", "SLAVE");
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("TestDB");
    criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    criteria.setPartition("TestDB_2%");
    criteria.setPartitionState("SLAVE");

    String josql =
        " SELECT DISTINCT mapSubKey AS 'subkey', mapValue AS 'mapValue' , getSimpleFieldValue(getZNRecordFromMap(:LIVEINSTANCESMAP, mapSubKey), 'SCN') AS 'SCN'"
            + " FROM org.apache.helix.josql.ZNRecordRow "
            + " WHERE mapKey LIKE 'TestDB_2%' "
            + " AND mapSubKey LIKE '%' "
            + " AND mapValue LIKE 'SLAVE' "
            + " AND mapSubKey IN ((SELECT [*]id FROM :LIVEINSTANCES)) "
            + " ORDER BY parseInt(getSimpleFieldValue(getZNRecordFromMap(:LIVEINSTANCESMAP, mapSubKey), 'SCN'))";

    Query josqlQuery = new Query();
    josqlQuery.setVariable("LIVEINSTANCES", liveInstances);
    josqlQuery.setVariable("LIVEINSTANCESMAP", liveInstanceMap);
    josqlQuery.addFunctionHandler(new ZNRecordRow());
    josqlQuery.addFunctionHandler(new ZNRecordJosqlFunctionHandler());
    josqlQuery.addFunctionHandler(new Integer(0));
    try {
      josqlQuery.parse(josql);
      QueryResults qr = josqlQuery.execute(ZNRecordRow.convertMapFields(externalView));
      @SuppressWarnings({
          "unchecked", "unused"
      })
      List<Object> result = qr.getResults();

    } catch (QueryParseException e) {
      e.printStackTrace();
    } catch (QueryExecutionException e) {
      e.printStackTrace();
    }

  }
}
