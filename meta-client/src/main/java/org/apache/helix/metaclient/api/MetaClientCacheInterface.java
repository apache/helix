package org.apache.helix.metaclient.api;

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

public interface MetaClientCacheInterface<T> extends MetaClientInterface<T> {

    /**
     * TrieNode class to store the children of the entries to be cached.
     */
    class TrieNode {
        // A mapping between trie key and children nodes.
        private Map<String, TrieNode> _children;
        // the complete path/prefix leading to the current node.
        private final String _path;
        private final String _nodeKey;

        public TrieNode(String path, String nodeKey) {
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

        public void addChild(String key, TrieNode node) {
            _children.put(key, node);
        }

        public TrieNode processPath(String path, boolean isCreate) {
            String[] pathComponents = path.split("/");
            TrieNode currentNode = this;
            TrieNode previousNode = null;

            for (int i = 1; i < pathComponents.length; i++) {
                String component = pathComponents[i];
                if (component.equals(_nodeKey)) {
                    // Skip the root node
                } else if (!currentNode.getChildren().containsKey(component)) {
                    if (isCreate) {
                        TrieNode newNode = new TrieNode(currentNode.getPath() + "/" + component, component);
                        currentNode.addChild(component, newNode);
                        previousNode = currentNode;
                        currentNode = newNode;
                    } else {
                        return currentNode;
                    }
                } else {
                    previousNode = currentNode;
                    currentNode = currentNode.getChildren().get(component);
                }
            }

            if (!isCreate && previousNode != null) {
                previousNode.getChildren().remove(currentNode.getNodeKey());
            }

            return currentNode;
        }
    }
}
