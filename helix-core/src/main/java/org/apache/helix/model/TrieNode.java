package org.apache.helix.model;

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
import java.util.Map;


public class TrieNode {
  // A mapping between trie key and children nodes.
  private Map<String, TrieNode> _children;

  // the complete path/prefix leading to the current node.
  private final String _path;

  private final String _nodeKey;

  TrieNode(String path, String nodeKey) {
    _path = path;
    _nodeKey = nodeKey;
    _children = new HashMap<>();
  }

  public Map<String, TrieNode> getChildren() {
    return _children;
  }

  public String getPath() {
    return _path;
  }

  public String getNodeKey() {
    return _nodeKey;
  }

  public void addChildrenMap(Map <String, TrieNode> children) {
    _children = children;
  }
  public void addChild(String key, TrieNode node) {
    _children.put(key, node);
  }
}