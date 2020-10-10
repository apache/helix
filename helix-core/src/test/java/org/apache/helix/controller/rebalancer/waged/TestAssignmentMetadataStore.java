package org.apache.helix.controller.rebalancer.waged;

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
import java.util.List;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAssignmentMetadataStore extends ZkTestBase {
  private static final int DEFAULT_BUCKET_SIZE = 50 * 1024; // 50KB
  private static final String BASELINE_KEY = "BASELINE";
  private static final String BEST_POSSIBLE_KEY = "BEST_POSSIBLE";

  protected static final String TEST_DB = "TestDB";
  protected HelixManager _manager;
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  private AssignmentMetadataStore _store;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    // create cluster manager
    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    // Create AssignmentMetadataStore. No version clean up to ensure the test result is stable.
    _store = new AssignmentMetadataStore(
        new ZkBucketDataAccessor(_manager.getMetadataStoreConnectionString(), DEFAULT_BUCKET_SIZE,
            Integer.MAX_VALUE), _manager.getClusterName());
  }

  @AfterClass
  public void afterClass() {
    if (_store != null) {
      _store.close();
    }
    if (_manager != null) {
      _manager.disconnect();
    }
    _gSetupTool.deleteCluster(CLUSTER_NAME);
  }

  /**
   * TODO: Reading baseline will be empty because AssignmentMetadataStore isn't being used yet by
   * the new rebalancer. Modify this integration test once the WAGED rebalancer
   * starts using AssignmentMetadataStore's persist APIs.
   * TODO: WAGED Rebalancer currently does NOT work with ZKClusterVerifier because verifier's
   * HelixManager is null, and that causes an NPE when instantiating AssignmentMetadataStore.
   */
  @Test
  public void testReadEmptyBaseline() {
    // This should be the first test. Assert there is no record in ZK.
    // Check that only one version exists
    Assert.assertEquals(getExistingVersionNumbers(BASELINE_KEY).size(), 0);
    Assert.assertEquals(getExistingVersionNumbers(BEST_POSSIBLE_KEY).size(), 0);
    // Read from cache and the result is empty.
    Assert.assertTrue(_store.getBaseline().isEmpty());
    Assert.assertTrue(_store.getBestPossibleAssignment().isEmpty());
  }

  /**
   * Test that if the old assignment and new assignment are the same,
   */
  @Test(dependsOnMethods = "testReadEmptyBaseline")
  public void testAvoidingRedundantWrite() {
    Map<String, ResourceAssignment> dummyAssignment = getDummyAssignment();

    // Call persist functions
    _store.persistBaseline(dummyAssignment);
    _store.persistBestPossibleAssignment(dummyAssignment);

    // Check that only one version exists
    Assert.assertEquals(getExistingVersionNumbers(BASELINE_KEY).size(), 1);
    Assert.assertEquals(getExistingVersionNumbers(BEST_POSSIBLE_KEY).size(), 1);

    // Call persist functions again
    _store.persistBaseline(dummyAssignment);
    _store.persistBestPossibleAssignment(dummyAssignment);

    // Check that only one version exists still
    Assert.assertEquals(getExistingVersionNumbers(BASELINE_KEY).size(), 1);
    Assert.assertEquals(getExistingVersionNumbers(BEST_POSSIBLE_KEY).size(), 1);
  }

  @Test(dependsOnMethods = "testAvoidingRedundantWrite")
  public void testAssignmentCache() {
    Map<String, ResourceAssignment> dummyAssignment = getDummyAssignment();
    // Call persist functions
    _store.persistBaseline(dummyAssignment);
    _store.persistBestPossibleAssignment(dummyAssignment);

    // Check that only one version exists
    Assert.assertEquals(getExistingVersionNumbers(BASELINE_KEY).size(), 1);
    Assert.assertEquals(getExistingVersionNumbers(BEST_POSSIBLE_KEY).size(), 1);

    // Same data in cache
    Assert.assertEquals(_store._bestPossibleAssignment, dummyAssignment);
    Assert.assertEquals(_store._globalBaseline, dummyAssignment);

    dummyAssignment.values().stream().forEach(assignment -> {
      assignment.addReplicaMap(new Partition("foo"), Collections.emptyMap());
    });

    // Call persist functions
    _store.persistBaseline(dummyAssignment);
    _store.persistBestPossibleAssignment(dummyAssignment);

    // Check that two versions exist
    Assert.assertEquals(getExistingVersionNumbers(BASELINE_KEY).size(), 2);
    Assert.assertEquals(getExistingVersionNumbers(BEST_POSSIBLE_KEY).size(), 2);

    // Same data in cache
    Assert.assertEquals(_store._bestPossibleAssignment, dummyAssignment);
    Assert.assertEquals(_store._globalBaseline, dummyAssignment);

    // Clear cache
    _store.reset();

    Assert.assertEquals(_store._bestPossibleAssignment, null);
    Assert.assertEquals(_store._globalBaseline, null);

    // Check the persisted data is not changed.
    Assert.assertEquals(getExistingVersionNumbers(BASELINE_KEY).size(), 2);
    Assert.assertEquals(getExistingVersionNumbers(BEST_POSSIBLE_KEY).size(), 2);
  }

  @Test(dependsOnMethods = "testAssignmentCache")
  void testClearAssignment() {
    // Check the persisted data is not empty
    List<String> baselineVersions = getExistingVersionNumbers(BASELINE_KEY);
    List<String> bestPossibleVersions = getExistingVersionNumbers(BEST_POSSIBLE_KEY);
    int baselineVersionCount = baselineVersions.size();
    int bestPossibleVersionCount = bestPossibleVersions.size();
    Assert.assertTrue(baselineVersionCount > 0);
    Assert.assertTrue(bestPossibleVersionCount > 0);

    _store.clearAssignmentMetadata();

    // 1. cache is cleaned up
    Assert.assertEquals(_store._bestPossibleAssignment, Collections.emptyMap());
    Assert.assertEquals(_store._globalBaseline, Collections.emptyMap());
    // 2. refresh the cache and then read from ZK again to ensure the persisted assignments is empty
    _store.reset();
    Assert.assertEquals(_store.getBaseline(), Collections.emptyMap());
    Assert.assertEquals(_store.getBestPossibleAssignment(), Collections.emptyMap());
    // 3. check that there is
    Assert.assertEquals(getExistingVersionNumbers(BASELINE_KEY).size(), baselineVersionCount + 1);
    Assert.assertEquals(getExistingVersionNumbers(BEST_POSSIBLE_KEY).size(), bestPossibleVersionCount + 1);
  }

  private Map<String, ResourceAssignment> getDummyAssignment() {
    // Generate a dummy assignment
    Map<String, ResourceAssignment> dummyAssignment = new HashMap<>();
    ResourceAssignment assignment = new ResourceAssignment(TEST_DB);
    Partition partition = new Partition(TEST_DB);
    Map<String, String> replicaMap = new HashMap<>();
    replicaMap.put(TEST_DB, TEST_DB);
    assignment.addReplicaMap(partition, replicaMap);
    dummyAssignment.put(TEST_DB, new ResourceAssignment(TEST_DB));
    return dummyAssignment;
  }

  /**
   * Returns a list of existing version numbers only.
   *
   * @param metadataType
   * @return
   */
  private List<String> getExistingVersionNumbers(String metadataType) {
    List<String> children = _baseAccessor
        .getChildNames("/" + CLUSTER_NAME + "/ASSIGNMENT_METADATA/" + metadataType,
            AccessOption.PERSISTENT);
    if (children == null) {
      children = Collections.EMPTY_LIST;
    }
    children.remove("LAST_SUCCESSFUL_WRITE");
    children.remove("LAST_WRITE");
    return children;
  }
}
