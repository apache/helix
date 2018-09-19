package org.apache.helix.manager.zk;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

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

public class TestZNRecordSerializer {
  /**
   * Test the normal case of serialize/deserialize where ZNRecord is well-formed
   */
  @Test
  public void basicTest() {
    ZNRecord record = new ZNRecord("testId");
    record.setMapField("k1", ImmutableMap.of("a", "b", "c", "d"));
    record.setMapField("k2", ImmutableMap.of("e", "f", "g", "h"));
    record.setListField("k3", ImmutableList.of("a", "b", "c", "d"));
    record.setListField("k4", ImmutableList.of("d", "e", "f", "g"));
    record.setSimpleField("k5", "a");
    record.setSimpleField("k5", "b");
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    ZNRecord result = (ZNRecord) serializer.deserialize(serializer.serialize(record));
    Assert.assertEquals(result, record);
  }


  @Test
  public void testNullFields() {
    ZNRecord record = new ZNRecord("testId");
    record.setMapField("K1", null);
    record.setListField("k2", null);
    record.setSimpleField("k3", null);
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    byte [] data = serializer.serialize(record);
    ZNRecord result = (ZNRecord) serializer.deserialize(data);

    Assert.assertEquals(result, record);
    Assert.assertNull(result.getMapField("K1"));
    Assert.assertNull(result.getListField("K2"));
    Assert.assertNull(result.getSimpleField("K3"));
    Assert.assertNull(result.getListField("K4"));
  }


  @Test (enabled = false)
  public void testPerformance() {
    ZNRecord record = createZnRecord();

    ZNRecordSerializer serializer1 = new ZNRecordSerializer();
    ZNRecordStreamingSerializer serializer2 = new ZNRecordStreamingSerializer();

    int loop = 100000;

    long start = System.currentTimeMillis();
    for (int i = 0; i < loop; i++) {
      serializer1.serialize(record);
    }
    System.out.println("ZNRecordSerializer serialize took " + (System.currentTimeMillis() - start) + " ms");

    byte[] data = serializer1.serialize(record);
    start = System.currentTimeMillis();
    for (int i = 0; i < loop; i++) {
      serializer1.deserialize(data);
    }
    System.out.println("ZNRecordSerializer deserialize took " + (System.currentTimeMillis() - start) + " ms");


    start = System.currentTimeMillis();
    for (int i = 0; i < loop; i++) {
      data = serializer2.serialize(record);
    }
    System.out.println("ZNRecordStreamingSerializer serialize took " + (System.currentTimeMillis() - start) + " ms");

    start = System.currentTimeMillis();
    for (int i = 0; i < loop; i++) {
      ZNRecord result = (ZNRecord) serializer2.deserialize(data);
    }
    System.out.println("ZNRecordStreamingSerializer deserialize took " + (System.currentTimeMillis() - start) + " ms");
  }


  ZNRecord createZnRecord() {
    ZNRecord record = new ZNRecord("testId");
    for (int i = 0; i < 400; i++) {
      Map<String, String> map = new HashMap<>();
      map.put("localhost_" + i, "Master");
      map.put("localhost_" + (i+1), "Slave");
      map.put("localhost_" + (i+2), "Slave");

      record.setMapField("partition_" + i, map);
      record.setListField("partition_" + i, Lists.<String>newArrayList(map.keySet()));
      record.setSimpleField("partition_" + i,  UUID.randomUUID().toString());
    }

    return record;
  }


  @Test (enabled = false)
  public void testParallelPerformance() throws ExecutionException, InterruptedException {
    final ZNRecord record = createZnRecord();

    final ZNRecordSerializer serializer1 = new ZNRecordSerializer();
    final ZNRecordStreamingSerializer serializer2 = new ZNRecordStreamingSerializer();

    int loop = 100000;

    ExecutorService executorService = Executors.newFixedThreadPool(10000);

    long start = System.currentTimeMillis();
    batchSerialize(serializer1, executorService, loop, record);
    System.out.println("ZNRecordSerializer serialize took " + (System.currentTimeMillis() - start) + " ms");

    byte[] data = serializer1.serialize(record);
    start = System.currentTimeMillis();
    batchSerialize(serializer2, executorService, loop, record);
    System.out.println("ZNRecordSerializer deserialize took " + (System.currentTimeMillis() - start) + " ms");


    start = System.currentTimeMillis();
    for (int i = 0; i < loop; i++) {
      data = serializer2.serialize(record);
    }
    System.out.println("ZNRecordStreamingSerializer serialize took " + (System.currentTimeMillis() - start) + " ms");

    start = System.currentTimeMillis();
    for (int i = 0; i < loop; i++) {
      ZNRecord result = (ZNRecord) serializer2.deserialize(data);
    }
    System.out.println("ZNRecordStreamingSerializer deserialize took " + (System.currentTimeMillis() - start) + " ms");
  }


  private void batchSerialize(final ZkSerializer serializer, ExecutorService executorService, int repeatTime, final ZNRecord record)
      throws ExecutionException, InterruptedException {
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < repeatTime; i++) {
      Future f = executorService.submit(new Runnable() {
        @Override public void run() {
          serializer.serialize(record);
        }
      });
      futures.add(f);
    }
    for (Future f : futures) {
      f.get();
    }
  }


  private void batchDeSerialize(final ZkSerializer serializer, ExecutorService executorService, int repeatTime, final byte [] data)
      throws ExecutionException, InterruptedException {
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < repeatTime; i++) {
      Future f = executorService.submit(new Runnable() {
        @Override public void run() {
          serializer.deserialize(data);
        }
      });
      futures.add(f);
    }
    for (Future f : futures) {
      f.get();
    }
  }

  /**
   * Test that simple, list, and map fields are initialized as empty even when not in json
   */
  @Test
  public void fieldAutoInitTest() {
    StringBuilder jsonString = new StringBuilder("{\n").append("\"id\": \"myId\"\n").append("}");
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    ZNRecord result = (ZNRecord) serializer.deserialize(jsonString.toString().getBytes());
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getId(), "myId");
    Assert.assertNotNull(result.getSimpleFields());
    Assert.assertTrue(result.getSimpleFields().isEmpty());
    Assert.assertNotNull(result.getListFields());
    Assert.assertTrue(result.getListFields().isEmpty());
    Assert.assertNotNull(result.getMapFields());
    Assert.assertTrue(result.getMapFields().isEmpty());
  }

  @Test
  public void testBasicCompression() {
    ZNRecord record = new ZNRecord("testId");
    int numPartitions = 1024;
    int replicas = 3;
    int numNodes = 100;
    Random random = new Random();
    for (int p = 0; p < numPartitions; p++) {
      Map<String, String> map = new HashMap<String, String>();
      for (int r = 0; r < replicas; r++) {
        map.put("host_" + random.nextInt(numNodes), "ONLINE");
      }
      record.setMapField("TestResource_" + p, map);
    }
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    byte[] serializedBytes;
    serializedBytes = serializer.serialize(record);
    int uncompressedSize = serializedBytes.length;
    System.out.println("raw serialized data length = " + serializedBytes.length);
    record.setSimpleField("enableCompression", "true");
    serializedBytes = serializer.serialize(record);
    int compressedSize = serializedBytes.length;
    System.out.println("compressed serialized data length = " + serializedBytes.length);
    System.out.printf("compression ratio: %.2f \n", (uncompressedSize * 1.0 / compressedSize));
    ZNRecord result = (ZNRecord) serializer.deserialize(serializedBytes);
    Assert.assertEquals(result, record);
  }

  @Test
  public void testCompression() {
    int runId = 1;
    while (runId < 20) {
      int numPartitions = runId * 1000;
      int replicas = 3;
      int numNodes = 100;
      Random random = new Random();
      ZNRecord record = new ZNRecord("testId");
      System.out.println("Partitions:" + numPartitions);
      for (int p = 0; p < numPartitions; p++) {
        Map<String, String> map = new HashMap<String, String>();
        for (int r = 0; r < replicas; r++) {
          map.put("host_" + random.nextInt(numNodes), "ONLINE");
        }
        record.setMapField("TestResource_" + p, map);
      }
      ZNRecordSerializer serializer = new ZNRecordSerializer();
      byte[] serializedBytes;
      record.setSimpleField("enableCompression", "true");
      serializedBytes = serializer.serialize(record);
      int compressedSize = serializedBytes.length;
      System.out.println("compressed serialized data length = " + compressedSize);
      ZNRecord result = (ZNRecord) serializer.deserialize(serializedBytes);
      Assert.assertEquals(result, record);
      runId = runId + 1;
    }
  }
}
