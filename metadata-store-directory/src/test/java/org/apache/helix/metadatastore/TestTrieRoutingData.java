package org.apache.helix.metadatastore;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.helix.metadatastore.datamodel.TrieRoutingData;
import org.apache.helix.metadatastore.exception.InvalidRoutingDataException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTrieRoutingData {
  private TrieRoutingData _trie;

  @Test
  public void testConstructionMissingRoutingData() {
    try {
      new TrieRoutingData(null);
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(e.getMessage().contains("routingData cannot be null or empty"));
    }
    try {
      new TrieRoutingData(Collections.emptyMap());
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(e.getMessage().contains("routingData cannot be null or empty"));
    }
  }

  /**
   * This test case is for the situation when there's only one sharding key and it's root.
   */
  @Test
  public void testConstructionSpecialCase() {
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put("realmAddress", Collections.singletonList("/"));
    TrieRoutingData trie;
    try {
      trie = new TrieRoutingData(routingData);
      Map<String, String> result = trie.getAllMappingUnderPath("/");
      Assert.assertEquals(result.size(), 1);
      Assert.assertEquals(result.get("/"), "realmAddress");
    } catch (InvalidRoutingDataException e) {
      Assert.fail("Not expecting InvalidRoutingDataException");
    }
  }

  @Test
  public void testConstructionShardingKeyNoLeadingSlash() {
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put("realmAddress1", Arrays.asList("/g", "/h/i", "/h/j"));
    routingData.put("realmAddress2", Arrays.asList("b/c/d", "/b/f"));
    routingData.put("realmAddress3", Collections.singletonList("/b/c/e"));
    try {
      new TrieRoutingData(routingData);
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(
          e.getMessage().contains("Sharding key does not have a leading \"/\" character: b/c/d"));
    }
  }

  @Test
  public void testConstructionRootAsShardingKeyInvalid() {
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put("realmAddress1", Arrays.asList("/a/b", "/"));
    try {
      new TrieRoutingData(routingData);
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(e.getMessage()
          .contains("There exist other sharding keys. Root cannot be a sharding key."));
    }
  }

  @Test
  public void testConstructionShardingKeyContainsAnother() {
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put("realmAddress1", Arrays.asList("/a/b", "/a/b/c"));
    try {
      new TrieRoutingData(routingData);
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(e.getMessage().contains(
          "/a/b/c cannot be a sharding key because /a/b is its parent key and is also a sharding key."));
    }
  }

  @Test
  public void testConstructionShardingKeyIsAPartOfAnother() {
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put("realmAddress1", Arrays.asList("/a/b/c", "/a/b"));
    try {
      new TrieRoutingData(routingData);
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(e.getMessage().contains(
          "/a/b cannot be a sharding key because it is a parent key to another sharding key."));
    }
  }

  /**
   * Constructing a trie that will also be reused for other tests
   * -----<empty>
   * ------/-|--\
   * -----b--g--h
   * ----/-\---/-\
   * ---c--f--i--j
   * --/-\
   * -d--e
   * Note: "g", "i", "j" lead to "realmAddress1"; "d", "f" lead to "realmAddress2"; "e" leads to
   * "realmAddress3"
   */
  @Test
  public void testConstructionNormal() {
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put("realmAddress1", Arrays.asList("/g", "/h/i", "/h/j"));
    routingData.put("realmAddress2", Arrays.asList("/b/c/d", "/b/f"));
    routingData.put("realmAddress3", Collections.singletonList("/b/c/e"));
    try {
      _trie = new TrieRoutingData(routingData);
    } catch (InvalidRoutingDataException e) {
      Assert.fail("Not expecting InvalidRoutingDataException");
    }
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetAllMappingUnderPathEmptyPath() {
    try {
      _trie.getAllMappingUnderPath("");
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("Provided path is empty or does not have a leading \"/\" character: "));
    }
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetAllMappingUnderPathNoLeadingSlash() {
    try {
      _trie.getAllMappingUnderPath("test");
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("Provided path is empty or does not have a leading \"/\" character: test"));
    }
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetAllMappingUnderPathFromRoot() {
    Map<String, String> result = _trie.getAllMappingUnderPath("/");
    Assert.assertEquals(result.size(), 6);
    Assert.assertEquals(result.get("/b/c/d"), "realmAddress2");
    Assert.assertEquals(result.get("/b/c/e"), "realmAddress3");
    Assert.assertEquals(result.get("/b/f"), "realmAddress2");
    Assert.assertEquals(result.get("/g"), "realmAddress1");
    Assert.assertEquals(result.get("/h/i"), "realmAddress1");
    Assert.assertEquals(result.get("/h/j"), "realmAddress1");
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetAllMappingUnderPathFromSecondLevel() {
    Map<String, String> result = _trie.getAllMappingUnderPath("/b");
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get("/b/c/d"), "realmAddress2");
    Assert.assertEquals(result.get("/b/c/e"), "realmAddress3");
    Assert.assertEquals(result.get("/b/f"), "realmAddress2");
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetAllMappingUnderPathFromLeaf() {
    Map<String, String> result = _trie.getAllMappingUnderPath("/b/c/d");
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get("/b/c/d"), "realmAddress2");
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetAllMappingUnderPathWrongPath() {
    Map<String, String> result = _trie.getAllMappingUnderPath("/b/c/d/g");
    Assert.assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetMetadataStoreRealmEmptyPath() {
    try {
      Assert.assertEquals(_trie.getMetadataStoreRealm(""), "realmAddress2");
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("Provided path is empty or does not have a leading \"/\" character: "));
    }
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetMetadataStoreRealmNoSlash() {
    try {
      Assert.assertEquals(_trie.getMetadataStoreRealm("b/c/d/x/y/z"), "realmAddress2");
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Provided path is empty or does not have a leading \"/\" character: b/c/d/x/y/z"));
    }
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetMetadataStoreRealm() {
    try {
      Assert.assertEquals(_trie.getMetadataStoreRealm("/b/c/d/x/y/z"), "realmAddress2");
    } catch (NoSuchElementException e) {
      Assert.fail("Not expecting NoSuchElementException");
    }
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetMetadataStoreRealmWrongPath() {
    try {
      _trie.getMetadataStoreRealm("/x/y/z");
      Assert.fail("Expecting NoSuchElementException");
    } catch (NoSuchElementException e) {
      Assert.assertTrue(
          e.getMessage().contains("The provided path is missing from the trie. Path: /x/y/z"));
    }
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetMetadataStoreRealmNoLeaf() {
    try {
      _trie.getMetadataStoreRealm("/b/c");
      Assert.fail("Expecting NoSuchElementException");
    } catch (NoSuchElementException e) {
      Assert.assertTrue(e.getMessage().contains("No leaf node found along the path. Path: /b/c"));
    }
  }
}
