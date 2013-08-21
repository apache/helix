package org.apache.helix.taskexecution;

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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;

public class Dag {
  private Map<String, Node> nodes = new HashMap<String, Dag.Node>();

  public static class Node {
    private String id;
    private int numPartitions;
    private Set<String> parentIds;

    public Node(String id, int numPartitions, Set<String> parentIds) {
      this.setId(id);
      this.setNumPartitions(numPartitions);
      this.setParentIds(parentIds);
    }

    public Node(String id, int numPartitions, String parentIdsStr) {
      this.setId(id);
      this.setNumPartitions(numPartitions);
      if (parentIdsStr != null && !parentIdsStr.trim().isEmpty()) {
        String tmp[] = parentIdsStr.split(",");
        parentIds = new HashSet<String>();
        parentIds.addAll(Arrays.asList(tmp));
      }
      this.setParentIds(parentIds);
    }

    public Node() {
      setId("");
      setNumPartitions(0);
      setParentIds(new HashSet<String>());
    }

    public String getId() {
      return id;
    }

    public int getNumPartitions() {
      return numPartitions;
    }

    public Set<String> getParentIds() {
      return parentIds;
    }

    public static Node fromJson(String json) throws Exception {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readValue(json, Node.class);
    }

    public String toJson() throws Exception {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.defaultPrettyPrintingWriter().writeValueAsString(this);
    }

    public void setId(String id) {
      this.id = id;
    }

    public void setNumPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    public void setParentIds(Set<String> parentIds) {
      this.parentIds = parentIds;
    }
  }

  public void addNode(Node node) {
    getNodes().put(node.getId(), node);
  }

  public Node getNode(String id) {
    return getNodes().get(id);
  }

  public Set<String> getNodeIds() {
    return getNodes().keySet();
  }

  public static Dag fromJson(String json) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(json, Dag.class);
  }

  public String toJson() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.defaultPrettyPrintingWriter().writeValueAsString(this);
  }

  public Map<String, Node> getNodes() {
    return nodes;
  }

  public void setNodes(Map<String, Node> nodes) {
    this.nodes = nodes;
  }
}
