package org.apache.helix.rest.server;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.Entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test class for swap completion API endpoints.
 * Tests various scenarios for canCompleteSwap and completeSwapIfPossible operations.
 */
public class TestCanSwapCompleteAPI extends AbstractTestClass {
  private static final String CLUSTER_NAME = "TestCluster_4";
  private static final String INSTANCE_NAME = CLUSTER_NAME + "localhost_12918";
  private BestPossibleExternalViewVerifier bestPossibleClusterVerifier;

  @BeforeClass
  public void beforeClass() {
    bestPossibleClusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkClient(_gZkClient).build();
  }

  /**
   * Tests canCompleteSwap when swap-out instance has no current state.
   * This should return true when preconditions are met.
   */
  @Test
  public void testCanCompleteSwap_NoResources() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    SwapPair swapPair = createSwapPair("_fresh_out", "_fresh_in", true);

    try {

      // EVACUATE swap-out instance
      Entity<String> empty = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
      evacuateInstance(swapPair.getSwapOut(), empty);

      // Verify evacuation completed successfully
      verifyEvacuationFinished(swapPair.getSwapOut(), empty);

      // Wait for current states to be drained
      Assert.assertTrue(TestHelper.verify(() -> getInstanceCurrentStates(swapPair.getSwapOut()).isEmpty(), TestHelper.WAIT_DURATION));

      // Verify canCompleteSwap returns true
      verifyCanCompleteSwap(swapPair.getSwapOut(), empty, true);

      // Verify completeSwapIfPossible returns true
      verifyCompleteSwapIfPossible(swapPair.getSwapOut(), empty, true);

      // Stop swap-out participant and verify final state
      swapPair.getOutParticipant().syncStop();
      Assert.assertTrue(TestHelper.verify(() -> getInstanceCurrentStates(swapPair.getSwapOut()).isEmpty(), TestHelper.WAIT_DURATION));
      clearMessages(swapPair.getSwapOut());
    } finally {
      swapPair.cleanup();
    }

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Tests canCompleteSwap when swap-in instance is offline.
   * This should return false since swap-in must be online for swap completion.
   */
  @Test
  public void testCanCompleteSwap_FalseWhenSwapInOffline() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    SwapPair swapPair = createSwapPair("_offline_in_out", "_offline_in_in", false);
    Entity<String> empty = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);

    try {
      // Start only swap-out participant, leaving swap-in offline
      swapPair.startOutParticipantOnly();

      // Evacuate swap-out and wait for drain
      evacuateInstance(swapPair.getSwapOut(), empty);
      TestHelper.verify(() -> getInstanceCurrentStates(swapPair.getSwapOut()).isEmpty(), TestHelper.WAIT_DURATION);

      // canCompleteSwap must return false since swap-in is offline
      verifyCanCompleteSwap(swapPair.getSwapOut(), empty, false);

      // Verify completeSwapIfPossible returns false
      verifyCompleteSwapIfPossible(swapPair.getSwapOut(), empty, false);
    } finally {
      swapPair.cleanup();
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }


  /**
   * Tests canCompleteSwap behavior with multiple resources on swap-out instance.
   * This verifies that the API correctly handles scenarios with multiple resources.
   */
  @Test
  public void testCanCompleteSwap_WithMultipleSwapOutResources() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    SwapPair swapPair = createSwapPair("_multi_resources_out", "_multi_resources_in", true);
    Entity<String> empty = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);

    // Create additional test resources to verify multi-resource handling
    String testResource1 = "TestResource1_" + System.currentTimeMillis();
    String testResource2 = "TestResource2_" + System.currentTimeMillis();
    addResource(CLUSTER_NAME, testResource1, NUM_PARTITIONS, "MasterSlave", 2, 3);
    addResource(CLUSTER_NAME, testResource2, NUM_PARTITIONS, "OnlineOffline", 2, 3);

    Assert.assertTrue(bestPossibleClusterVerifier.verifyByPolling());

    try {
      // Evacuate swap-out instance
      evacuateInstance(swapPair.getSwapOut(), empty);

      // Verify canCompleteSwap returns false (expected due to multiple resources)
      verifyCanCompleteSwap(swapPair.getSwapOut(), empty, false);

      // Verify completeSwapIfPossible also returns false
      verifyCompleteSwapIfPossible(swapPair.getSwapOut(), empty, false);
    } finally {
      swapPair.cleanup();
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Gets the current state assignments for a specific instance.
   */
  private Map<String, String> getInstanceCurrentStates(String instanceName) {
    Map<String, String> assignment = new HashMap<>();
    for (ExternalView ev : getEVs().values()) {
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap.containsKey(instanceName)) {
          assignment.put(partition, stateMap.get(instanceName));
        }
      }
    }
    return assignment;
  }

  /**
   * Gets all external views for the cluster.
   */
  private Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<>();
    for (String db : _resourcesMap.get(CLUSTER_NAME)) {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  /**
   * Creates a swap pair with matching logical IDs and optionally starts participants.
   */
  private SwapPair createSwapPair(String swapOutSuffix, String swapInSuffix, boolean startParticipants) throws JsonProcessingException {
    String swapOut = INSTANCE_NAME + swapOutSuffix;
    String swapIn = INSTANCE_NAME + swapInSuffix;

    // Create instance configs
    createInstanceConfig(swapOut);
    createInstanceConfig(swapIn);

    // Set matching logical IDs
    setMatchingLogicalIds(swapOut, swapIn);

    SwapPair pair = new SwapPair(swapOut, swapIn);

    if (startParticipants) {
      pair.startParticipants();
    }

    return pair;
  }

  /**
   * Creates an instance configuration and adds it to the cluster.
   */
  private void createInstanceConfig(String instanceName) throws JsonProcessingException {
    InstanceConfig config = new InstanceConfig(instanceName);
    Entity<String> entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(config.getRecord()), MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}").format(CLUSTER_NAME, instanceName).put(this, entity);
  }

  /**
   * Sets matching logical IDs on both swap instances using the cluster's end node key.
   */
  private void setMatchingLogicalIds(String swapOut, String swapIn) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    String endNodeKey = ClusterTopologyConfig.createFromClusterConfig(clusterConfig).getEndNodeType();
    String logicalIdValue = "logicalId_" + System.currentTimeMillis();

    setInstanceLogicalId(swapOut, endNodeKey, logicalIdValue);
    setInstanceLogicalId(swapIn, endNodeKey, logicalIdValue);
  }

  /**
   * Sets the logical ID for a specific instance.
   */
  private void setInstanceLogicalId(String instanceName, String endNodeKey, String logicalIdValue) {
    InstanceConfig config = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);
    Map<String, String> domain = config.getDomainAsMap();
    if (domain == null) {
      domain = new HashMap<>();
    }
    domain.put(endNodeKey, logicalIdValue);
    config.setDomain(buildDomainString(domain));
    _configAccessor.setInstanceConfig(CLUSTER_NAME, instanceName, config);
  }

  /**
   * Builds a domain string from a domain map.
   */
  private String buildDomainString(Map<String, String> domain) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, String> e : domain.entrySet()) {
      if (!first) sb.append(", ");
      sb.append(e.getKey()).append("=").append(e.getValue());
      first = false;
    }
    return sb.toString();
  }

  /**
   * Helper class to manage swap pair lifecycle.
   */
  private class SwapPair {
    private final String swapOut;
    private final String swapIn;
    private MockParticipantManager outParticipant;
    private MockParticipantManager inParticipant;

    public SwapPair(String swapOut, String swapIn) {
      this.swapOut = swapOut;
      this.swapIn = swapIn;
    }

    public void startParticipants() {
      inParticipant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, swapIn);
      inParticipant.syncStart();
      _mockParticipantManagers.add(inParticipant);

      outParticipant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, swapOut);
      outParticipant.syncStart();
      _mockParticipantManagers.add(outParticipant);
    }

    public void startOutParticipantOnly() {
      outParticipant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, swapOut);
      outParticipant.syncStart();
      _mockParticipantManagers.add(outParticipant);
    }

    public void cleanup() {
      if (inParticipant != null && inParticipant.isConnected()) {
        inParticipant.syncStop();
      }
      if (outParticipant != null && outParticipant.isConnected()) {
        outParticipant.syncStop();
      }
      delete("clusters/" + CLUSTER_NAME + "/instances/" + swapOut, Response.Status.OK.getStatusCode());
      delete("clusters/" + CLUSTER_NAME + "/instances/" + swapIn, Response.Status.OK.getStatusCode());
    }

    public String getSwapOut() { return swapOut; }
    @SuppressWarnings("unused") // May be used in future extensions
    public String getSwapIn() { return swapIn; }
    public MockParticipantManager getOutParticipant() { return outParticipant; }
  }

  /**
   * Evacuates an instance by setting its operation to EVACUATE.
   */
  private void evacuateInstance(String instanceName, Entity<String> empty) {
    new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=EVACUATE")
        .format(CLUSTER_NAME, instanceName).post(this, empty);
  }

  /**
   * Verifies that evacuation has finished for an instance.
   */
  private void verifyEvacuationFinished(String instanceName, Entity<String> empty) throws JsonProcessingException {
    Response evac = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=isEvacuateFinished")
        .format(CLUSTER_NAME, instanceName).post(this, empty);
    Assert.assertEquals(evac.getStatus(), Response.Status.OK.getStatusCode());

    Map<String, Boolean> evacMap = (Map<String, Boolean>) OBJECT_MAPPER.readValue(evac.readEntity(String.class), Map.class);
    Assert.assertTrue(evacMap.get("successful"), "Evacuation should have finished successfully");
  }

  /**
   * Verifies the canCompleteSwap API response.
   */
  private void verifyCanCompleteSwap(String instanceName, Entity<String> empty, boolean expectedResult) throws JsonProcessingException {
    Response canComplete = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=canCompleteSwap")
        .format(CLUSTER_NAME, instanceName).post(this, empty);
    Assert.assertEquals(canComplete.getStatus(), Response.Status.OK.getStatusCode());

    Map<String, Object> canCompleteMap = (Map<String, Object>) OBJECT_MAPPER.readValue(canComplete.readEntity(String.class), Map.class);
    Assert.assertEquals((boolean) canCompleteMap.get("successful"), expectedResult,
        "canCompleteSwap should return " + expectedResult);
  }

  /**
   * Verifies the completeSwapIfPossible API response.
   */
  private void verifyCompleteSwapIfPossible(String instanceName, Entity<String> empty, boolean expectedResult) throws JsonProcessingException {
    Response complete = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=completeSwapIfPossible")
        .format(CLUSTER_NAME, instanceName).post(this, empty);
    Assert.assertEquals(complete.getStatus(), Response.Status.OK.getStatusCode());

    Map<String, Object> completeMap = (Map<String, Object>) OBJECT_MAPPER.readValue(complete.readEntity(String.class), Map.class);
    Assert.assertEquals((boolean) completeMap.get("successful"), expectedResult,
        "completeSwapIfPossible should return " + expectedResult);
  }

  private void clearMessages(String instance) {
    String msgPath = org.apache.helix.PropertyPathBuilder.instanceMessage(CLUSTER_NAME, instance);
    if (_gZkClient.exists(msgPath)) {
      for (String m : _gZkClient.getChildren(msgPath)) {
        _gZkClient.delete(msgPath + "/" + m);
      }
    }
  }
}
