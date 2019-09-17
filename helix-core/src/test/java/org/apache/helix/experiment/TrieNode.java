package org.apache.helix.experiment;

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
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixRebalanceException;

public abstract class TrieNode<K> {
  private final String id;
  private Map<String, K> children = new HashMap<>();

  public TrieNode(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public Map<String, K> getChildren() {
    return children;
  }

  // public <T extends TrieNode> T get(String[] paths, Class<T> targetClass) {
  // if (paths.length == 0) {
  // return (T) this;
  // }
  //
  // String query = paths[0];
  // String[] subQueries = Arrays.asList(paths).subList(1, paths.length).toArray(new String[0]);
  // if (children.containsKey(query)) {
  // return (T) children.get(query).get(subQueries, targetClass);
  // }
  //
  // return null;
  // }

  public abstract void addChildren(K child);

  public abstract boolean assign(InstanceTrie instanceTrie, PartitionTrie partitionTrie)
      throws HelixRebalanceException;

  public abstract boolean release(InstanceTrie instanceTrie, PartitionTrie partitionTrie)
      throws HelixRebalanceException;
}
