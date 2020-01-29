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
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTrieRoutingData {
  // TODO: add constructor related tests after constructor is finished

  @Test
  public void testGetAllMappingUnderPathFromRoot() {
    TrieRoutingData trie = constructTestTrie();
    Map<String, String> result = trie.getAllMappingUnderPath("/");
    Assert.assertEquals(result.size(), 4);
    Assert.assertEquals(result.get("/b/c/d"), "zkRealmAddressD");
    Assert.assertEquals(result.get("/b/c/e"), "zkRealmAddressE");
    Assert.assertEquals(result.get("/b/f"), "zkRealmAddressF");
    Assert.assertEquals(result.get("/g"), "zkRealmAddressG");
  }

  @Test
  public void testGetAllMappingUnderPathFromSecondLevel() {
    TrieRoutingData trie = constructTestTrie();
    Map<String, String> result = trie.getAllMappingUnderPath("/b");
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get("/b/c/d"), "zkRealmAddressD");
    Assert.assertEquals(result.get("/b/c/e"), "zkRealmAddressE");
    Assert.assertEquals(result.get("/b/f"), "zkRealmAddressF");
  }

  @Test
  public void testGetAllMappingUnderPathFromLeaf() {
    TrieRoutingData trie = constructTestTrie();
    Map<String, String> result = trie.getAllMappingUnderPath("/b/c/d");
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get("/b/c/d"), "zkRealmAddressD");
  }

  @Test
  public void testGetAllMappingUnderPathWrongPath() {
    TrieRoutingData trie = constructTestTrie();
    Map<String, String> result = trie.getAllMappingUnderPath("/b/c/d/g");
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testGetZkRealm() {
    TrieRoutingData trie = constructTestTrie();
    Assert.assertEquals(trie.getZkRealm("/b/c/d/x/y/z"), "zkRealmAddressD");
  }

  @Test
  public void testGetZkRealmWrongPath() {
    TrieRoutingData trie = constructTestTrie();
    try {
      trie.getZkRealm("/x/y/z");
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("the provided zkPath is missing from the trie"));
    }
  }

  @Test
  public void testGetZkRealmNoLeaf() {
    TrieRoutingData trie = constructTestTrie();
    try {
      trie.getZkRealm("/b/c");
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("no leaf node found along the zkPath"));
    }
  }


  /**
   * Constructing a trie for testing purposes
   * -----<empty>
   * ------/ \
   * -----b  g
   * ----/ \
   * ---c  f
   * --/ \
   * -d  e
   */
  private TrieRoutingData constructTestTrie() {
    TrieRoutingData.TrieNode nodeD = new TrieRoutingData.TrieNode(Collections.emptyMap(), "d", true, "zkRealmAddressD");
    TrieRoutingData.TrieNode nodeE = new TrieRoutingData.TrieNode(Collections.emptyMap(), "e", true, "zkRealmAddressE");
    TrieRoutingData.TrieNode nodeF = new TrieRoutingData.TrieNode(Collections.emptyMap(), "f", true, "zkRealmAddressF");
    TrieRoutingData.TrieNode nodeG = new TrieRoutingData.TrieNode(Collections.emptyMap(), "g", true, "zkRealmAddressG");
    TrieRoutingData.TrieNode nodeC = new TrieRoutingData.TrieNode(new HashMap<String, TrieRoutingData.TrieNode>() {
      {
        put("d", nodeD);
        put("e", nodeE);
      }
    }, "c", false, "");
    TrieRoutingData.TrieNode nodeB = new TrieRoutingData.TrieNode(new HashMap<String, TrieRoutingData.TrieNode>() {
      {
        put("c", nodeC);
        put("f", nodeF);
      }
    }, "b", false, "");
    TrieRoutingData.TrieNode root = new TrieRoutingData.TrieNode(new HashMap<String, TrieRoutingData.TrieNode>() {
      {
        put("b", nodeB);
        put("g", nodeG);
      }
    }, "", false, "");

    return new TrieRoutingData(root);
  }
}
