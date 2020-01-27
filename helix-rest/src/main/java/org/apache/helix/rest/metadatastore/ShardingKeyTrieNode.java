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
import java.util.Map;

/**
 * A trie node that is used to represent Zk path sharding keys. Terminal nodes contain ZkRealm
 * addresses.
 */
public class ShardingKeyTrieNode {
  /**
   * Children trie nodes of this trie node. Each node is identified by their name.
   */
  private final Map<String, ShardingKeyTrieNode> _children;
  /**
   * Boolean value dictating if this trie node is a terminal node.
   */
  private final boolean _isLeaf;
  /**
   * This trie node's name.
   */
  private final String _name;
  /**
   * ZkRealm address corresponding to this trie node. The value is only valid when the node is a
   * terminal node.
   */
  private final String _zkRealmAddress;

  public ShardingKeyTrieNode(Map<String, ShardingKeyTrieNode> children, boolean isLeaf, String name,
      String zkRealmAddress) {
    _children = children;
    _isLeaf = isLeaf;
    _name = name;
    _zkRealmAddress = zkRealmAddress;
  }

  public Map<String, ShardingKeyTrieNode> getChildren() {
    return Collections.unmodifiableMap(_children);
  }

  public boolean isLeaf() {
    return _isLeaf;
  }

  public String getName() {
    return _name;
  }

  public String getZkRealmAddress() {
    return _zkRealmAddress;
  }

  public static class Builder {
    private Map<String, ShardingKeyTrieNode> _children;
    private boolean _isLeaf;
    private String _name;
    private String _zkRealmAddress;

    public ShardingKeyTrieNode build() {
      validate();
      return new ShardingKeyTrieNode(_children, _isLeaf, _name, _zkRealmAddress);
    }

    public Builder setChildren(final Map<String, ShardingKeyTrieNode> children) {
      _children = children;
      return this;
    }

    public Builder setLeaf(final boolean isLeaf) {
      _isLeaf = isLeaf;
      return this;
    }

    public Builder setName(final String name) {
      _name = name;
      return this;
    }

    public Builder setZkRealmAddress(final String zkRealmAddress) {
      _zkRealmAddress = zkRealmAddress;
      return this;
    }

    /**
     * A validation function to the ShardingKeyTrieNode that's getting built. If the name is null or
     * empty, the trie node is not valid; if the node is a terminal node and the ZkRealm address is
     * null or empty, the trie node is not valid.
     */
    private void validate() {
      if (_name == null || _name.isEmpty()) {
        throw new IllegalArgumentException("name cannot be null or empty");
      }
      if (_isLeaf && (_zkRealmAddress == null || _zkRealmAddress.isEmpty())) {
        throw new IllegalArgumentException(
            "zkRealmAddress cannot be null or empty when the node is a terminal node");
      }
    }
  }
}
