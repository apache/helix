package org.apache.helix.zookeeper.util;

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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHttpRoutingDataReader extends ZkTestBase {
  private MockMetadataStoreDirectoryServer _msdsServer;
  private Map<String, Collection<String>> _testRawRoutingData;
  private final String _host = "localhost";
  private final int _port = 1991;
  private final String _namespace = "TestHttpRoutingDataReader";

  @BeforeClass
  public void beforeClass() throws IOException {
    // Create fake routing data
    _testRawRoutingData = new HashMap<>();
    _testRawRoutingData
        .put("zk-0", ImmutableSet.of("/sharding-key-0", "/sharding-key-1", "/sharding-key-2"));
    _testRawRoutingData
        .put("zk-1", ImmutableSet.of("/sharding-key-3", "/sharding-key-4", "/sharding-key-5"));
    _testRawRoutingData
        .put("zk-2", ImmutableSet.of("/sharding-key-6", "/sharding-key-7", "/sharding-key-8"));

    // Start MockMSDS
    _msdsServer =
        new MockMetadataStoreDirectoryServer(_host, _port, _namespace, _testRawRoutingData);
    _msdsServer.startServer();

    // Register the endpoint as a System property
    String msdsEndpoint = "http://" + _host + ":" + _port + "/admin/v2/namespaces/" + _namespace;
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, msdsEndpoint);
  }

  @AfterClass
  public void afterClass() {
    _msdsServer.stopServer();
  }

  @Test
  public void testGetRawRoutingData() throws IOException {
    Map<String, List<String>> rawRoutingData = HttpRoutingDataReader.getRawRoutingData();
    _testRawRoutingData
        .forEach((realm, keys) -> Assert.assertEquals(rawRoutingData.get(realm), keys));
  }

  @Test(dependsOnMethods = "testGetRawRoutingData")
  public void testGetMetadataStoreRoutingData() throws IOException, InvalidRoutingDataException {
    MetadataStoreRoutingData data = HttpRoutingDataReader.getMetadataStoreRoutingData();
    Map<String, String> allMappings = data.getAllMappingUnderPath("/");
    Map<String, Set<String>> groupedMappings = allMappings.entrySet().stream().collect(Collectors
        .groupingBy(Map.Entry::getValue,
            Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));
    _testRawRoutingData.forEach((realm, keys) -> {
      // Two way containsAll because AssertEquals on two set collections is buggy in that
      // it will fail if the ordering of elements is not equal (we just want to compare contents)
      Assert.assertTrue(groupedMappings.get(realm).containsAll(keys));
      Assert.assertTrue(keys.containsAll(groupedMappings.get(realm)));
    });
  }

  /**
   * Test that the static methods in HttpRoutingDataReader returns consistent results even though MSDS's data have been updated.
   */
  @Test(dependsOnMethods = "testGetMetadataStoreRoutingData")
  public void testStaticMapping() throws IOException, InvalidRoutingDataException {
    // Modify routing data
    String newRealm = "newRealm";
    _testRawRoutingData.put(newRealm, ImmutableSet.of("/newKey"));

    // Kill MSDS and restart with a new mapping
    _msdsServer.stopServer();
    _msdsServer =
        new MockMetadataStoreDirectoryServer(_host, _port, _namespace, _testRawRoutingData);
    _msdsServer.startServer();

    // HttpRoutingDataReader should still return old data because it's static
    // Make sure the results don't contain the new realm
    Map<String, List<String>> rawRoutingData = HttpRoutingDataReader.getRawRoutingData();
    Assert.assertFalse(rawRoutingData.containsKey(newRealm));

    MetadataStoreRoutingData data = HttpRoutingDataReader.getMetadataStoreRoutingData();
    Map<String, String> allMappings = data.getAllMappingUnderPath("/");
    Map<String, Set<String>> groupedMappings = allMappings.entrySet().stream().collect(Collectors
        .groupingBy(Map.Entry::getValue,
            Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));
    Assert.assertFalse(groupedMappings.containsKey(newRealm));
  }
}
