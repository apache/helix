package org.apache.helix.tools.ClusterVerifiers;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.ReadOnlyWagedRebalancer;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.util.RebalanceUtil;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * verifier that the ExternalViews of given resources (or all resources in the cluster)
 * match its best possible mapping states.
 */
public class BestPossibleExternalViewVerifier extends ZkHelixClusterVerifier {
  private static Logger LOG = LoggerFactory.getLogger(BestPossibleExternalViewVerifier.class);

  private final Map<String, Map<String, String>> _errStates;
  private final Set<String> _resources;
  private final Set<String> _expectLiveInstances;
  private final ResourceControllerDataProvider _dataProvider;

  /**
   * Deprecated - please use the Builder to construct this class.
   * @param zkAddr
   * @param clusterName
   * @param resources
   * @param errStates
   * @param expectLiveInstances
   */
  @Deprecated
  public BestPossibleExternalViewVerifier(String zkAddr, String clusterName, Set<String> resources,
      Map<String, Map<String, String>> errStates, Set<String> expectLiveInstances) {
    this(zkAddr, clusterName, resources, errStates, expectLiveInstances, 0);
  }

  @Deprecated
  public BestPossibleExternalViewVerifier(String zkAddr, String clusterName, Set<String> resources,
      Map<String, Map<String, String>> errStates, Set<String> expectLiveInstances, int waitTillVerify) {
    super(zkAddr, clusterName, waitTillVerify);
    _errStates = errStates;
    _resources = resources;
    _expectLiveInstances = expectLiveInstances;
    _dataProvider = new ResourceControllerDataProvider();
    // _zkClient should be closed with BestPossibleExternalViewVerifier
  }

  /**
   * Deprecated - please use the Builder to construct this class.
   * @param zkClient
   * @param clusterName
   * @param resources
   * @param errStates
   * @param expectLiveInstances
   */
  @Deprecated
  public BestPossibleExternalViewVerifier(RealmAwareZkClient zkClient, String clusterName,
      Set<String> resources, Map<String, Map<String, String>> errStates,
      Set<String> expectLiveInstances) {
    this(zkClient, clusterName, errStates, resources, expectLiveInstances, 0, true);
  }

  @Deprecated
  public BestPossibleExternalViewVerifier(RealmAwareZkClient zkClient, String clusterName,
      Set<String> resources, Map<String, Map<String, String>> errStates,
      Set<String> expectLiveInstances, int waitTillVerify) {
    // usesExternalZkClient = true because ZkClient is given by the caller
    // at close(), we will not close this ZkClient because it might be being used elsewhere
    this(zkClient, clusterName, errStates, resources, expectLiveInstances, waitTillVerify, true);
  }

  private BestPossibleExternalViewVerifier(RealmAwareZkClient zkClient, String clusterName,
      Map<String, Map<String, String>> errStates, Set<String> resources,
      Set<String> expectLiveInstances, int waitPeriodTillVerify, boolean usesExternalZkClient) {
    // Initialize BestPossibleExternalViewVerifier with usesExternalZkClient = false so that
    // BestPossibleExternalViewVerifier::close() would close ZkClient to prevent thread leakage
    super(zkClient, clusterName, usesExternalZkClient, waitPeriodTillVerify);
    // Deep copy data from Builder
    _errStates = new HashMap<>();
    if (errStates != null) {
      errStates.forEach((k, v) -> _errStates.put(k, new HashMap<>(v)));
    }
    _resources = resources == null ? new HashSet<>() : new HashSet<>(resources);
    _expectLiveInstances =
        expectLiveInstances == null ? new HashSet<>() : new HashSet<>(expectLiveInstances);
    _dataProvider = new ResourceControllerDataProvider();
  }

  public static class Builder extends ZkHelixClusterVerifier.Builder<Builder> {
    private final String _clusterName;
    private Map<String, Map<String, String>> _errStates;
    private Set<String> _resources;
    private Set<String> _expectLiveInstances;
    private RealmAwareZkClient _zkClient;

    public Builder(String clusterName) {
      _clusterName = clusterName;
    }

    public BestPossibleExternalViewVerifier build() {
      if (_clusterName == null) {
        throw new IllegalArgumentException("Cluster name is missing!");
      }

      // _usesExternalZkClient == true
      if (_zkClient != null) {
        return new BestPossibleExternalViewVerifier(_zkClient, _clusterName, _errStates, _resources,
            _expectLiveInstances, _waitPeriodTillVerify, true);
      }
      // _usesExternalZkClient == false
      if (_realmAwareZkConnectionConfig == null || _realmAwareZkClientConfig == null) {
        // For backward-compatibility
        return new BestPossibleExternalViewVerifier(_zkAddress, _clusterName, _resources,
            _errStates, _expectLiveInstances, _waitPeriodTillVerify);
      }

      validate();
      return new BestPossibleExternalViewVerifier(
          createZkClient(RealmAwareZkClient.RealmMode.SINGLE_REALM, _realmAwareZkConnectionConfig,
              _realmAwareZkClientConfig, _zkAddress), _clusterName, _errStates, _resources,
          _expectLiveInstances, _waitPeriodTillVerify, false);
    }

    public String getClusterName() {
      return _clusterName;
    }

    public Map<String, Map<String, String>> getErrStates() {
      return _errStates;
    }

    public Builder setErrStates(Map<String, Map<String, String>> errStates) {
      _errStates = errStates;
      return this;
    }

    public Set<String> getResources() {
      return _resources;
    }

    public Builder setResources(Set<String> resources) {
      _resources = resources;
      return this;
    }

    public Set<String> getExpectLiveInstances() {
      return _expectLiveInstances;
    }

    public Builder setExpectLiveInstances(Set<String> expectLiveInstances) {
      _expectLiveInstances = expectLiveInstances;
      return this;
    }

    public String getZkAddr() {
      return _zkAddress;
    }

    public Builder setZkClient(RealmAwareZkClient zkClient) {
      _zkClient = zkClient;
      return this;
    }
  }

  @Override
  public boolean verify(long timeout) {
    return verifyByZkCallback(timeout);
  }

  @Override
  public boolean verifyByZkCallback(long timeout) {
    waitTillVerify();

    List<ClusterVerifyTrigger> triggers = new ArrayList<ClusterVerifyTrigger>();

    // setup triggers
    if (_resources != null && !_resources.isEmpty()) {
      for (String resource : _resources) {
        triggers
            .add(new ClusterVerifyTrigger(_keyBuilder.idealStates(resource), true, false, false));
        triggers
            .add(new ClusterVerifyTrigger(_keyBuilder.externalView(resource), true, false, false));
      }

    } else {
      triggers.add(new ClusterVerifyTrigger(_keyBuilder.idealStates(), false, true, true));
      triggers.add(new ClusterVerifyTrigger(_keyBuilder.externalViews(), false, true, true));
    }

    return verifyByCallback(timeout, triggers);
  }

  @Override
  protected synchronized boolean verifyState() {
    try {
      PropertyKey.Builder keyBuilder = _accessor.keyBuilder();

      _dataProvider.requireFullRefresh();
      _dataProvider.refresh(_accessor);
      _dataProvider.setClusterEventId("ClusterStateVerifier");

      Map<String, IdealState> idealStates = new HashMap<>(_dataProvider.getIdealStates());

      // filter out all resources that use Task state model
      idealStates.entrySet()
          .removeIf(pair -> pair.getValue().getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME));

      // verify live instances.
      if (_expectLiveInstances != null && !_expectLiveInstances.isEmpty()) {
        Set<String> actualLiveNodes = _dataProvider.getLiveInstances().keySet();
        if (!_expectLiveInstances.equals(actualLiveNodes)) {
          LOG.warn("Live instances are not as expected. Actual live nodes: " + actualLiveNodes
              .toString());
          return false;
        }
      }

      Map<String, ExternalView> extViews =
          _accessor.getChildValuesMap(keyBuilder.externalViews(), true);
      if (extViews == null) {
        extViews = Collections.emptyMap();
      }

      // Filter resources if requested
      if (_resources != null && !_resources.isEmpty()) {
        idealStates.keySet().retainAll(_resources);
        extViews.keySet().retainAll(_resources);
      }

      // if externalView is not empty and idealState doesn't exist
      // add empty idealState for the resource
      for (String resource : extViews.keySet()) {
        if (!idealStates.containsKey(resource)) {
          ExternalView ev = extViews.get(resource);
          IdealState is = new IdealState(resource);
          is.getRecord().setSimpleFields(ev.getRecord().getSimpleFields());
          idealStates.put(resource, is);
        }
      }

      // calculate best possible state
      BestPossibleStateOutput bestPossOutput = calcBestPossState(_dataProvider, _resources);
      Map<String, Map<Partition, Map<String, String>>> bestPossStateMap =
          bestPossOutput.getStateMap();

      // set error states
      if (_errStates != null) {
        for (String resourceName : _errStates.keySet()) {
          Map<String, String> partErrStates = _errStates.get(resourceName);
          for (String partitionName : partErrStates.keySet()) {
            String instanceName = partErrStates.get(partitionName);

            if (!bestPossStateMap.containsKey(resourceName)) {
              bestPossStateMap.put(resourceName, new HashMap<Partition, Map<String, String>>());
            }
            Partition partition = new Partition(partitionName);
            if (!bestPossStateMap.get(resourceName).containsKey(partition)) {
              bestPossStateMap.get(resourceName).put(partition, new HashMap<String, String>());
            }
            bestPossStateMap.get(resourceName).get(partition)
                .put(instanceName, HelixDefinedState.ERROR.toString());
          }
        }
      }

      for (String resourceName : idealStates.keySet()) {
        IdealState is = idealStates.get(resourceName);
        ExternalView extView = extViews.get(resourceName);
        if (extView == null) {
          if (is.isExternalViewDisabled()) {
            continue;
          }
          LOG.warn("externalView for " + resourceName
              + " is not available, check if best possible state is available.");
          extView = new ExternalView(resourceName);
        }

        // step 0: remove empty map and DROPPED state from best possible state
        PartitionStateMap bpStateMap =
            bestPossOutput.getPartitionStateMap(resourceName);

        StateModelDefinition stateModelDef = _dataProvider.getStateModelDef(is.getStateModelDefRef());
        if (stateModelDef == null) {
          LOG.error(
              "State model definition " + is.getStateModelDefRef() + " for resource not found!" + is
                  .getResourceName());
          return false;
        }

        boolean result = verifyExternalView(extView, bpStateMap, stateModelDef);

        if (!result) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("verifyExternalView fails for " + resourceName + "! ExternalView: " + extView
                + " BestPossibleState: " + bpStateMap);
          } else {
            LOG.warn("verifyExternalView fails for " + resourceName
                + "! ExternalView does not match BestPossibleState");
          }
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      LOG.error("exception in verification", e);
      return false;
    }
  }

  private boolean verifyExternalView(ExternalView externalView,
      PartitionStateMap bestPossibleState, StateModelDefinition stateModelDef) {
    Set<String> ignoreStates = new HashSet<>(
        Arrays.asList(stateModelDef.getInitialState(), HelixDefinedState.DROPPED.toString()));

    Map<String, Map<String, String>> bestPossibleStateMap =
        convertBestPossibleState(bestPossibleState);

    removeEntryWithIgnoredStates(bestPossibleStateMap.entrySet().iterator(), ignoreStates);

    Map<String, Map<String, String>> externalViewMap = externalView.getRecord().getMapFields();
    removeEntryWithIgnoredStates(externalViewMap.entrySet().iterator(), ignoreStates);

    return externalViewMap.equals(bestPossibleStateMap);
  }

  private void removeEntryWithIgnoredStates(
      Iterator<Map.Entry<String, Map<String, String>>> partitionInstanceStateMapIter,
      Set<String> ignoredStates) {
    while (partitionInstanceStateMapIter.hasNext()) {
      Map.Entry<String, Map<String, String>> entry = partitionInstanceStateMapIter.next();
      Map<String, String> instanceStateMap = entry.getValue();
      // remove instances with DROPPED and OFFLINE state
      Iterator<Map.Entry<String, String>> insIter = instanceStateMap.entrySet().iterator();
      while (insIter.hasNext()) {
        String state = insIter.next().getValue();
        if (ignoredStates.contains(state)) {
          insIter.remove();
        }
      }
      if (instanceStateMap.isEmpty()) {
        partitionInstanceStateMapIter.remove();
      }
    }
  }

  private Map<String, Map<String, String>> convertBestPossibleState(
      PartitionStateMap bestPossibleState) {
    Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
    for (Partition partition : bestPossibleState.getStateMap().keySet()) {
      result.put(partition.getPartitionName(), bestPossibleState.getPartitionMap(partition));
    }
    return result;
  }

  /**
   * calculate the best possible state note that DROPPED states are not checked since when
   * kick off the BestPossibleStateCalcStage we are providing an empty current state map
   *
   * @param cache
   * @param resources
   * @return
   * @throws Exception
   */
  private BestPossibleStateOutput calcBestPossState(ResourceControllerDataProvider cache, Set<String> resources)
      throws Exception {
    ClusterEvent event = new ClusterEvent(_clusterName, ClusterEventType.StateVerifier);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    RebalanceUtil.runStage(event, new ResourceComputationStage());

    if (resources != null && !resources.isEmpty()) {
      // Filtering out all non-required resources
      final Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
      resourceMap.keySet().retainAll(resources);
      event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);

      final Map<String, Resource> resourceMapToRebalance =
          event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
      resourceMapToRebalance.keySet().retainAll(resources);
      event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMapToRebalance);
    }

    RebalanceUtil.runStage(event, new CurrentStateComputationStage());
    // Note the readOnlyWagedRebalancer is just for one time usage

    try (ZkBucketDataAccessor zkBucketDataAccessor = new ZkBucketDataAccessor(_zkClient);
        DryrunWagedRebalancer dryrunWagedRebalancer = new DryrunWagedRebalancer(zkBucketDataAccessor,
            cache.getClusterName(), cache.getClusterConfig().getGlobalRebalancePreference())) {
      event.addAttribute(AttributeName.STATEFUL_REBALANCER.name(), dryrunWagedRebalancer);
      RebalanceUtil.runStage(event, new BestPossibleStateCalcStage());
    }

    return event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
  }

  @Override
  public String toString() {
    String verifierName = getClass().getSimpleName();
    return verifierName + "(" + _clusterName + "@" + _zkClient + "@resources["
       + (_resources != null ? Arrays.toString(_resources.toArray()) : "") + "])";
  }

  // TODO: to clean up, finalize is deprecated in Java 9
  @Override
  public void finalize() {
    close();
    super.finalize();
  }

  private static class DryrunWagedRebalancer extends ReadOnlyWagedRebalancer implements AutoCloseable {
    public DryrunWagedRebalancer(ZkBucketDataAccessor zkBucketDataAccessor, String clusterName,
        Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences) {
      super(zkBucketDataAccessor, clusterName, preferences);
    }

    @Override
    protected Map<String, ResourceAssignment> computeBestPossibleAssignment(
        ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
        Set<String> activeNodes, CurrentStateOutput currentStateOutput,
        RebalanceAlgorithm algorithm) throws HelixRebalanceException {
      return getBestPossibleAssignment(getAssignmentMetadataStore(), currentStateOutput,
          resourceMap.keySet());
    }
  }
}
