package org.apache.helix.rest.metadatastore;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javassist.NotFoundException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestTrieRoutingData {
  // TODO: add constructor related tests after constructor is finished

  @Test
  public void testGetAllMappingUnderPathFromRoot() {
    TrieRoutingData trie = constructTestTrie();
    Map<String, String> result = trie.getAllMappingUnderPath("/");
    Assert.assertEquals(result.size(), 4);
    Assert.assertEquals(result.get("/b/c/d"), "realmAddressD");
    Assert.assertEquals(result.get("/b/c/e"), "realmAddressE");
    Assert.assertEquals(result.get("/b/f"), "realmAddressF");
    Assert.assertEquals(result.get("/g"), "realmAddressG");
  }

  @Test
  public void testGetAllMappingUnderPathFromSecondLevel() {
    TrieRoutingData trie = constructTestTrie();
    Map<String, String> result = trie.getAllMappingUnderPath("/b");
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get("/b/c/d"), "realmAddressD");
    Assert.assertEquals(result.get("/b/c/e"), "realmAddressE");
    Assert.assertEquals(result.get("/b/f"), "realmAddressF");
  }

  @Test
  public void testGetAllMappingUnderPathFromLeaf() {
    TrieRoutingData trie = constructTestTrie();
    Map<String, String> result = trie.getAllMappingUnderPath("/b/c/d");
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get("/b/c/d"), "realmAddressD");
  }

  @Test
  public void testGetAllMappingUnderPathWrongPath() {
    TrieRoutingData trie = constructTestTrie();
    Map<String, String> result = trie.getAllMappingUnderPath("/b/c/d/g");
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testGetMetadataStoreRealm() {
    TrieRoutingData trie = constructTestTrie();
    try {
      Assert.assertEquals(trie.getMetadataStoreRealm("/b/c/d/x/y/z"), "realmAddressD");
    } catch (NotFoundException e) {
      Assert.fail("Not expecting NotFoundException");
    }
  }

  @Test
  public void testGetMetadataStoreRealmWrongPath() {
    TrieRoutingData trie = constructTestTrie();
    try {
      trie.getMetadataStoreRealm("/x/y/z");
      Assert.fail("Expecting NotFoundException");
    } catch (NotFoundException e) {
      Assert.assertTrue(e.getMessage().contains("the provided path is missing from the trie"));
    }
  }

  @Test
  public void testGetMetadataStoreRealmNoLeaf() {
    TrieRoutingData trie = constructTestTrie();
    try {
      trie.getMetadataStoreRealm("/b/c");
      Assert.fail("Expecting NotFoundException");
    } catch (NotFoundException e) {
      Assert.assertTrue(e.getMessage().contains("no leaf node found along the path"));
    }
  }

  /**
   * Constructing a trie for testing purposes
   * -----<empty>
   * ------/--\
   * -----b---g
   * ----/-\
   * ---c--f
   * --/-\
   * -d--e
   */
  private TrieRoutingData constructTestTrie() {
    TrieRoutingData.TrieNode nodeD =
        new TrieRoutingData.TrieNode(Collections.emptyMap(), "/b/c/d", true, "realmAddressD");
    TrieRoutingData.TrieNode nodeE =
        new TrieRoutingData.TrieNode(Collections.emptyMap(), "/b/c/e", true, "realmAddressE");
    TrieRoutingData.TrieNode nodeF =
        new TrieRoutingData.TrieNode(Collections.emptyMap(), "/b/f", true, "realmAddressF");
    TrieRoutingData.TrieNode nodeG =
        new TrieRoutingData.TrieNode(Collections.emptyMap(), "/g", true, "realmAddressG");
    TrieRoutingData.TrieNode nodeC =
        new TrieRoutingData.TrieNode(new HashMap<String, TrieRoutingData.TrieNode>() {
          {
            put("d", nodeD);
            put("e", nodeE);
          }
        }, "c", false, "");
    TrieRoutingData.TrieNode nodeB =
        new TrieRoutingData.TrieNode(new HashMap<String, TrieRoutingData.TrieNode>() {
          {
            put("c", nodeC);
            put("f", nodeF);
          }
        }, "b", false, "");
    TrieRoutingData.TrieNode root =
        new TrieRoutingData.TrieNode(new HashMap<String, TrieRoutingData.TrieNode>() {
          {
            put("b", nodeB);
            put("g", nodeG);
          }
        }, "", false, "");

    return new TrieRoutingData(root);
  }
}
