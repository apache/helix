package org.apache.helix.controller.rebalancer.topology;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Node implements Comparable<Node> {
  private String _name;
  private String _type;
  private long _id;
  private long _weight;

  private LinkedHashMap<String, Node> _children = new LinkedHashMap<String, Node>();
  private Node _parent;

  private boolean _failed;

  public Node() {

  }

  public Node(Node node) {
    _name = node.getName();
    _type = node.getType();
    _id = node.getId();
    _weight = node.getWeight();
    _failed = node.isFailed();
  }

  public String getName() {
    return _name;
  }

  public void setName(String name) {
    _name = name;
  }

  public String getType() {
    return _type;
  }

  public void setType(String type) {
    _type = type;
  }

  public long getId() {
    return _id;
  }

  public void setId(long id) {
    _id = id;
  }

  public long getWeight() {
    return _weight;
  }

  public void setWeight(long weight) {
    _weight = weight;
  }

  public void addWeight(long weight) { _weight += weight; }

  public boolean isFailed() {
    return _failed;
  }

  public void setFailed(boolean failed) {
    if (!isLeaf()) {
      throw new UnsupportedOperationException("you cannot set failed on a non-leaf!");
    }
    _failed = failed;
  }

  public List<Node> getChildren() {
    return new ArrayList<Node>(_children.values());
  }

  /**
   * Add a child, if there exists a child with the same name, will replace it.
   *
   * @param child
   */
  public void addChild(Node child) {
    _children.put(child.getName(), child);
  }

  /**
   * Has child with given name.
   * @param name
   * @return
   */
  public boolean hasChild(String name) {
    return _children.containsKey(name);
  }

  /**
   * Get child node with given name.
   *
   * @param name
   * @return
   */
  public Node getChild(String name) {
    return _children.get(name);
  }

  public boolean isLeaf() {
    return _children == null || _children.isEmpty();
  }

  public Node getParent() {
    return _parent;
  }

  public void setParent(Node parent) {
    _parent = parent;
  }

  /**
   * Returns all child nodes that match the type. Returns itself if this node matches it. If no
   * child matches the type, an empty list is returned.
   */
  protected List<Node> findChildren(String type) {
    List<Node> nodes = new ArrayList<Node>();
    if (_type.equalsIgnoreCase(type)) {
      nodes.add(this);
    } else if (!isLeaf()) {
      for (Node child: _children.values()) {
        nodes.addAll(child.findChildren(type));
      }
    }
    return nodes;
  }

  /**
   * Returns the number of all child nodes that match the type. Returns 1 if this node matches it.
   * Returns 0 if no child matches the type.
   */
  public int getChildrenCount(String type) {
    int count = 0;
    if (_type.equalsIgnoreCase(type)) {
      count++;
    } else if (!isLeaf()) {
      for (Node child: _children.values()) {
        count += child.getChildrenCount(type);
      }
    }
    return count;
  }

  /**
   * Returns the top-most ("root") node from this node. If this node itself does not have a parent,
   * returns itself.
   */
  public Node getRoot() {
    Node node = this;
    while (node.getParent() != null) {
      node = node.getParent();
    }
    return node;
  }

  @Override
  public String toString() {
    return _name + ":" + _id;
  }

  @Override
  public int hashCode() {
    return _name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Node)) {
      return false;
    }
    Node that = (Node)obj;
    return _name.equals(that.getName());
  }

  @Override
  public int compareTo(Node o) {
    return _name.compareTo(o.getName());
  }
}
