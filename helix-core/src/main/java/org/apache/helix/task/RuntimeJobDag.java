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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RuntimeJobDag is a job DAG that provides the job iterator functionality at runtime (when jobs are
 * actually being assigned per job category). This is to support assignment of jobs based on their
 * categories and quotas. RuntimeJobDag uses the list scheduling algorithm using ready-list and
 * inflight-list to return jobs available for scheduling.
 * NOTE: RuntimeJobDag is not thread-safe.
 */
public class RuntimeJobDag extends JobDag {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeJobDag.class);
  private static final int DEFAULT_NUM_PARALLEL_JOBS = 1;

  // For job iterator functionality
  private ArrayDeque<String> _readyJobList; // Jobs ready to be scheduled
  private Set<String> _inflightJobList; // Jobs that are scheduled but not yet finished
  private boolean _hasDagChanged; // Flag for DAG modification for job queues; if true, ready-list
                                  // must be re-computed
  private Map<String, Set<String>> _successorMap; // Two dependency maps for populating ready-list
  private Map<String, Set<String>> _predecessorMap; // when jobs are finished
  private boolean _isJobQueue;
  private int _numParallelJobs;
  private String _lastJob;
  private int _version; // The version of the workflow config znode that is used to construct this RuntimeJobDag

  /**
   * Constructor for Job DAG.
   */
  public RuntimeJobDag() {
    // For job list iterator scheduling
    _readyJobList = new ArrayDeque<>();
    _inflightJobList = new HashSet<>();
    _hasDagChanged = true;
    _version = 0;
  }

  public RuntimeJobDag(JobDag jobDag, boolean isJobQueue, int numParallelJobs, int version) {
    this._childrenToParents = jobDag.getChildrenToParents();
    this._parentsToChildren = jobDag.getParentsToChildren();
    this._allNodes = jobDag.getAllNodes();
    this._isJobQueue = isJobQueue;
    this._numParallelJobs = numParallelJobs <= 0 ? DEFAULT_NUM_PARALLEL_JOBS : numParallelJobs;
    this._version = version;
    generateJobList();
  }

  public int getVersion() {
    return this._version;
  }

  @Override
  public void addParentToChild(String parent, String child) {
    _hasDagChanged = true;
    super.addParentToChild(parent, child);
  }

  @Override
  public void addNode(String node) {
    _hasDagChanged = true;
    super.addNode(node);
  }

  /**
   * Remove a node from the DAG and set the changed flag to true.
   * @param job
   * @param maintainDependency: if true, the removed job's parent and child node will be linked
   *          together,
   *          otherwise, the job will be removed directly without modifying the existing dependency
   *          links.
   */
  public void removeNode(String job, boolean maintainDependency) {
    _hasDagChanged = true;
    super.removeNode(job, maintainDependency);
  }

  /**
   * Returns true if the job iterator has jobs. If the job DAG has been modified, re-generates
   * ready-list.
   * @return true if the iterator has more elements
   */
  public boolean hasNextJob() {
    if (_hasDagChanged) {
      generateJobList(); // Regenerate the ready list
    }
    return !_readyJobList.isEmpty();
  }

  /**
   * Returns next unscheduled job from the job iterator. If the job DAG has been modified,
   * re-generates ready-list.
   * @return job name. Null if the readyJobList is empty.
   */
  @Override
  public String getNextJob() {
    if (_hasDagChanged) {
      generateJobList(); // Regenerate the ready list
    }
    // If list is empty, return null
    if (_readyJobList.isEmpty()) {

      return null;
    }
    String nextJob = _readyJobList.poll();
    _inflightJobList.add(nextJob);
    _lastJob = nextJob;



    return nextJob;
  }

  /**
   * Removes a finished job from the job iterator. If the DAG has been changed, it returns false and leaves an error log. The job must be in the FINISHED state before this call.
   * @param job name of the job to be removed
   */
  public boolean finishJob(String job) {
    if (_hasDagChanged) {
      LOG.warn("Job DAG has been modified; Cannot finish job: {}", job);
      return false;
    }
    if (!_inflightJobList.remove(job)) {
      // this job is not in in-flight list
      LOG.warn(
          "Job: {} has either finished already, never been scheduled, or been removed from DAG",
          job);
    }
    // Add finished job's successors to ready-list

    if (_isJobQueue) {
      // If it is a jobQueue, there should be a check to make sure that the a job has not been added
      // to the _readyJobList multiple times. This check is necessary because once the controller
      // switch happens, the _readyJobList and _inflightJobList will be created from scratch. In this
      // case, since there might be many jobs that have been finished before, we do not want to have a
      // job several times to the _readyJobList. If _readyJobList has multiple instances of the same
      // job, it can compromise the functionality of the parallel jobs.
      while (_lastJob != null && _parentsToChildren.containsKey(_lastJob)) {
        String nextJob = _parentsToChildren.get(_lastJob).iterator().next();
        if (!_readyJobList.contains(nextJob)) {
          _readyJobList.offer(nextJob);
          break;
        }
        _lastJob = nextJob;
      }
    } else if (_successorMap.containsKey(job)) {
        for (String successor : _successorMap.get(job)) {
          // Remove finished job from predecessor map
        if (_predecessorMap.containsKey(successor)) {
          Set<String> predecessors = _predecessorMap.get(successor);
          predecessors.remove(job);
          // Successor must have no predecessors before being added to ready-list
          if (predecessors.isEmpty()) {
            _readyJobList.offer(successor);
          }
        }
      }
    }
    _successorMap.remove(job);
    return true;
  }

  /**
   * Must be called BEFORE using job iterator method and AFTER a full DAG has been added.
   * Resets all job lists and regenerates the ready list. Also, sets the job DAG change flag back to
   * false.
   * The reason this method cannot be in the constructor is that DAG is empty at initialization,
   * and only the client will know when they are done with adding individual jobs to DAG and job
   * list is ready to be created.
   */
  public void generateJobList() {
    resetJobListAndDependencyMaps();
    computeIndependentNodes();
    _readyJobList.addAll(_independentNodes);
    if (_isJobQueue && !_readyJobList.isEmpty()) {
      // For job queue, only get number of parallel jobs to run in the ready list.
      for (int i = 1; i < _numParallelJobs; i++) {
        Set<String> children = _parentsToChildren.get(_readyJobList.peekLast());
        if (children == null) {
          break;
        }
        _readyJobList.offer(children.iterator().next());
      }
    }
    _hasDagChanged = false;
  }

  /**
   * Re-initialize ready and in-flight lists. Also perform a deep-copy of dependency maps because
   * iterator methods will modify successor and predecessor relationships.
   */
  private void resetJobListAndDependencyMaps() {
    _readyJobList = new ArrayDeque<>();
    _inflightJobList = new HashSet<>();
    _successorMap = new HashMap<>();
    _predecessorMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : _parentsToChildren.entrySet()) {
      _successorMap.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }
    for (Map.Entry<String, Set<String>> entry : _childrenToParents.entrySet()) {
      _predecessorMap.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }
  }

  public Set<String> getInflightJobList() {
    return new HashSet<>(_inflightJobList);
  }
}
