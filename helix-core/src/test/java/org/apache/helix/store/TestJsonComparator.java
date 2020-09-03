package org.apache.helix.store;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Date;

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestJsonComparator {
  @Test(groups = {
    "unitTest"
  })
  public void testJsonComparator() {
    System.out.println("START TestJsonComparator at " + new Date(System.currentTimeMillis()));

    ZNRecord record = new ZNRecord("id1");
    PropertyJsonComparator<ZNRecord> comparator =
        new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    AssertJUnit.assertTrue(comparator.compare(null, null) == 0);
    AssertJUnit.assertTrue(comparator.compare(null, record) == -1);
    AssertJUnit.assertTrue(comparator.compare(record, null) == 1);
    System.out.println("END TestJsonComparator at " + new Date(System.currentTimeMillis()));
  }
}
