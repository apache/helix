package org.apache.helix;

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

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZNRecordBucketizer {
  @Test
  public void testZNRecordBucketizer() {
    final int bucketSize = 3;
    ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(bucketSize);
    String[] partitionNames = {
        "TestDB_0", "TestDB_1", "TestDB_2", "TestDB_3", "TestDB_4"
    };
    for (int i = 0; i < partitionNames.length; i++) {
      String partitionName = partitionNames[i];
      String bucketName = bucketizer.getBucketName(partitionName);
      int startBucketNb = i / bucketSize * bucketSize;
      int endBucketNb = startBucketNb + bucketSize - 1;
      String expectBucketName = "TestDB_p" + startBucketNb + "-p" + endBucketNb;
      System.out.println("Expect: " + expectBucketName + ", actual: " + bucketName);
      Assert.assertEquals(expectBucketName, bucketName);

    }

    // ZNRecord record = new ZNRecord("TestDB");
    // record.setSimpleField("key0", "value0");
    // record.setSimpleField("key1", "value1");
    // record.setListField("TestDB_0", Arrays.asList("localhost_00", "localhost_01"));
    // record.setListField("TestDB_1", Arrays.asList("localhost_10", "localhost_11"));
    // record.setListField("TestDB_2", Arrays.asList("localhost_20", "localhost_21"));
    // record.setListField("TestDB_3", Arrays.asList("localhost_30", "localhost_31"));
    // record.setListField("TestDB_4", Arrays.asList("localhost_40", "localhost_41"));
    //
    // System.out.println(bucketizer.bucketize(record));

  }
}
