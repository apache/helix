package org.apache.helix.task;

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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a convenient way to construct, traverse,
 * and validate a job dependency graph
 */
public class JobDag {
  private static final Logger LOG = LoggerFactory.getLogger(JobDag.class);

  @JsonProperty("parentsToChildren")
  protected Map<String, Set<String>> _parentsToChildren;

  @JsonProperty("childrenToParents")
  protected Map<String, Set<String>> _childrenToParents;

  @JsonProperty("allNodes")
  protected Set<String> _allNodes;
  protected Set<String> _independentNodes; // Un-parented nodes are stored to avoid repeated calculation
  // unless there is a DAG modification

  public static final JobDag EMPTY_DAG = new JobDag();

  protected Iterator<String> _jobIterator;

  /**
   * Constructor for Job DAG.
   */
  public JobDag() {
    // TreeMap and TreeSet (vs Hash equivalents) are used to maintain ordering of jobs. Note that
    // nomenclature for jobs (namespacing jobs) is WORKFLOW_NAME + DATETIME + ..., this ordering
    // determines the order in which ZNodes are stored in Zookeeper, and therefore the order in
    // which they are processed by the Controller pipeline.
    _parentsToChildren = new TreeMap<>();
    _childrenToParents = new TreeMap<>();
    _allNodes = new TreeSet<>();
  }

  public void addParentToChild(String parent, String child) {
    if (!_parentsToChildren.containsKey(parent)) {
      _parentsToChildren.put(parent, new TreeSet<String>());
    }
    _parentsToChildren.get(parent).add(child);

    if (!_childrenToParents.containsKey(child)) {
      _childrenToParents.put(child, new TreeSet<String>());
    }
    _childrenToParents.get(child).add(parent);

    _allNodes.add(parent);
    _allNodes.add(child);
  }

  private void removeParentToChild(String parent, String child) {
    if (_parentsToChildren.containsKey(parent)) {
      Set<String> children = _parentsToChildren.get(parent);
      children.remove(child);
      if (children.isEmpty()) {
        _parentsToChildren.remove(parent);
      }
    }

    if (_childrenToParents.containsKey(child)) {
      Set<String> parents = _childrenToParents.get(child);
      parents.remove(parent);
      if (parents.isEmpty()) {
        _childrenToParents.remove(child);
      }
    }
  }

  public void addNode(String node) {
    _allNodes.add(node);
  }

  /**
   * Checks if there are any lingering inter-node dependency for a node prior to removal.
   */
  private void removeNode(String node) {
    if (_parentsToChildren.containsKey(node) || _childrenToParents.containsKey(node)) {
      throw new IllegalStateException(
          "The node is either a parent or a child of other node, could not be deleted");
    }

    _allNodes.remove(node);
  }

  /**
   * Remove a node from the DAG.
   * @param job
   * @param maintainDependency: if true, the removed job's parent and child node will be linked together,
   *                          otherwise, the job will be removed directly without modifying the existing dependency links.
   */
  public void removeNode(String job, boolean maintainDependency) {
    if (!_allNodes.contains(job)) {
      LOG.info("Could not delete job {} from DAG, node does not exist", job);
      return;
    }
    if (maintainDependency) {
      String parent = null;
      String child = null;
      // remove the node from the queue
      for (String n : _allNodes) {
        if (getDirectChildren(n).contains(job)) {
          parent = n;
          removeParentToChild(parent, job);
        } else if (getDirectParents(n).contains(job)) {
          child = n;
          removeParentToChild(job, child);
        }
      }
      if (parent != null && child != null) {
        addParentToChild(parent, child);
      }
      removeNode(job);
    } else {
      for (String child : getDirectChildren(job)) {
        getChildrenToParents().get(child).remove(job);
      }
      for (String parent : getDirectParents(job)) {
        getParentsToChildren().get(parent).remove(job);
      }
      _childrenToParents.remove(job);
      _parentsToChildren.remove(job);
      removeNode(job);
    }
  }

  public Map<String, Set<String>> getParentsToChildren() {
    return _parentsToChildren;
  }

  public Map<String, Set<String>> getChildrenToParents() {
    return _childrenToParents;
  }

  public Set<String> getAllNodes() {
    return _allNodes;
  }

  public Set<String> getDirectChildren(String node) {
    if (!_parentsToChildren.containsKey(node)) {
      return Collections.emptySet();
    }
    return _parentsToChildren.get(node);
  }

  public Set<String> getDirectParents(String node) {
    if (!_childrenToParents.containsKey(node)) {
      return Collections.emptySet();
    }
    return _childrenToParents.get(node);
  }

  public Set<String> getAncestors(String node) {
    Set<String> ancestors = new HashSet<>();
    Set<String> current = Collections.singleton(node);

    while (!current.isEmpty()) {
      Set<String> next = new HashSet<>();
      for (String currentNode : current) {
        next.addAll(getDirectParents(currentNode));
      }
      ancestors.addAll(next);
      current = next;
    }

    return ancestors;
  }

  public String toJson() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }

  public static JobDag fromJson(String json) {
    try {
      return new ObjectMapper().readValue(json, JobDag.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse json " + json + " into job dag");
    }
  }

  public int size() {
    return _allNodes.size();
  }

  /**
   * Checks that dag contains no cycles and all nodes are reachable.
   */
  public void validate() {
    computeIndependentNodes();
    Set<String> prevIteration = _independentNodes;

    // visit children nodes up to max iteration count, by which point we should have exited
    // naturally
    Set<String> allNodesReached = new HashSet<>();
    int iterationCount = 0;
    int maxIterations = _allNodes.size() + 1;

    while (!prevIteration.isEmpty() && iterationCount < maxIterations) {
      // construct set of all children reachable from prev iteration
      Set<String> thisIteration = new HashSet<>();
      for (String node : prevIteration) {
        thisIteration.addAll(getDirectChildren(node));
      }

      allNodesReached.addAll(prevIteration);
      prevIteration = thisIteration;
      iterationCount++;
    }

    allNodesReached.addAll(prevIteration);

    if (iterationCount >= maxIterations) {
      throw new IllegalArgumentException("DAG invalid: cycles detected");
    }

    if (!allNodesReached.containsAll(_allNodes)) {
      throw new IllegalArgumentException("DAG invalid: unreachable nodes found. Reachable set is "
          + allNodesReached);
    }
  }

  /**
   * Computes nodes that are independent stores it in _independentNodes.
   * Independent nodes are a set of un-parented nodes, which implies jobs corresponding to these
   * nodes may be scheduled without worrying about the status of other jobs.
   */
  protected void computeIndependentNodes() {
    _independentNodes = new HashSet<>();
    for (String node : _allNodes) {
      if (!_childrenToParents.containsKey(node)) {
        _independentNodes.add(node);
      }
    }
  }

  @JsonIgnore
  public String getNextJob() {
    if (_jobIterator == null) {
      _jobIterator = _allNodes.iterator();
    }
    if (_jobIterator.hasNext()) {
      return _jobIterator.next();
    }
    return null;
  }
}
