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

import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestShardingKeyTrieNode {

  /**
   * Testing the proper creation case, building a sharding key trie like so:
   *     a
   *    / \
   *   b  e
   *  / \
   * c  d
   */
  @Test
  public void testCreation() {
    ShardingKeyTrieNode nodeC = new ShardingKeyTrieNode.Builder().setLeaf(true).setName("c")
        .setZkRealmAddress("testZkRealmAddressC").build();
    ShardingKeyTrieNode nodeD = new ShardingKeyTrieNode.Builder().setLeaf(true).setName("d")
        .setZkRealmAddress("testZkRealmAddressD").build();
    ShardingKeyTrieNode nodeB = new ShardingKeyTrieNode.Builder().setLeaf(false).setName("b")
        .setChildren(new HashMap<String, ShardingKeyTrieNode>() {
          {
            put("c", nodeC);
            put("d", nodeD);
          }
        }).build();
    ShardingKeyTrieNode nodeE = new ShardingKeyTrieNode.Builder().setLeaf(true).setName("e")
        .setZkRealmAddress("testZkRealmAddressE").build();
    ShardingKeyTrieNode nodeA = new ShardingKeyTrieNode.Builder().setLeaf(false).setName("a")
        .setChildren(new HashMap<String, ShardingKeyTrieNode>() {
          {
            put("b", nodeB);
            put("e", nodeE);
          }
        }).build();

    Assert.assertEquals(nodeA.getName(), "a");
    Assert.assertFalse(nodeA.isLeaf());
    ShardingKeyTrieNode testNodeB = nodeA.getChildren().get("b");
    Assert.assertEquals(testNodeB.getName(), "b");
    Assert.assertFalse(testNodeB.isLeaf());
    ShardingKeyTrieNode testNodeC = testNodeB.getChildren().get("c");
    Assert.assertEquals(testNodeC.getName(), "c");
    Assert.assertTrue(testNodeC.isLeaf());
    Assert.assertEquals(testNodeC.getZkRealmAddress(), "testZkRealmAddressC");
    ShardingKeyTrieNode testNodeD = testNodeB.getChildren().get("d");
    Assert.assertEquals(testNodeD.getName(), "d");
    Assert.assertTrue(testNodeD.isLeaf());
    Assert.assertEquals(testNodeD.getZkRealmAddress(), "testZkRealmAddressD");
    ShardingKeyTrieNode testNodeE = nodeA.getChildren().get("e");
    Assert.assertEquals(testNodeE.getName(), "e");
    Assert.assertTrue(testNodeE.isLeaf());
    Assert.assertEquals(testNodeE.getZkRealmAddress(), "testZkRealmAddressE");
  }

  @Test
  public void testValidationFailEmptyName() {
    try {
      ShardingKeyTrieNode testNode = new ShardingKeyTrieNode.Builder().setLeaf(false).build();
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("name cannot be null or empty"));
    }
  }

  @Test
  public void testValidationFailEmptyZkRealmAddress() {
    try {
      ShardingKeyTrieNode testNode =
          new ShardingKeyTrieNode.Builder().setLeaf(true).setName("test").build();
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("zkRealmAddress cannot be null or empty when the node is a terminal node"));
    }
  }

  @Test
  public void testValidationFailNonEmptyChildren() {
    try {
      ShardingKeyTrieNode testChildNode = new ShardingKeyTrieNode.Builder().setLeaf(true).setName("child")
          .setZkRealmAddress("testZkRealmAddressChild").build();
      ShardingKeyTrieNode testNode = new ShardingKeyTrieNode.Builder().setLeaf(true).setName("test")
          .setZkRealmAddress("testZkRealmAddress")
          .setChildren(new HashMap<String, ShardingKeyTrieNode>() {
            {
              put("child", testChildNode);
            }
          }).build();
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("children needs to be empty when the node is a terminal node"));
    }
  }


  @Test
  public void testGetZkRealmAddressException() {
    try {
      ShardingKeyTrieNode testNode = new ShardingKeyTrieNode.Builder().setLeaf(false).setName("test")
          .setZkRealmAddress("testZkRealmAddress").build();
      testNode.getZkRealmAddress();
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage()
          .contains("only leaf nodes have meaningful zkRealmAddress"));
    }
  }
}
