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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.HelixDefinedState;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * verifier that the ExternalViews of given resources (or all resources in the cluster)
 * match its best possible mapping states.
 */
public class BestPossibleExternalViewVerifier extends ZkHelixClusterVerifier {
  private static Logger LOG = LoggerFactory.getLogger(BestPossibleExternalViewVerifier.class);

  private final Map<String, Map<String, String>> _errStates;
  private final Set<String> _resources;
  private final Set<String> _expectLiveInstances;
  private final ClusterDataCache _clusterDataCache;

  public BestPossibleExternalViewVerifier(String zkAddr, String clusterName, Set<String> resources,
      Map<String, Map<String, String>> errStates, Set<String> expectLiveInstances) {
    super(zkAddr, clusterName);
    _errStates = errStates;
    _resources = resources;
    _expectLiveInstances = expectLiveInstances;
    _clusterDataCache = new ClusterDataCache();
  }

  public BestPossibleExternalViewVerifier(ZkClient zkClient, String clusterName,
      Set<String> resources, Map<String, Map<String, String>> errStates,
      Set<String> expectLiveInstances) {
    super(zkClient, clusterName);
    _errStates = errStates;
    _resources = resources;
    _expectLiveInstances = expectLiveInstances;
    _clusterDataCache = new ClusterDataCache();
  }
  
  public static class Builder {
    private String _clusterName;
    private Map<String, Map<String, String>> _errStates;
    private Set<String> _resources;
    private Set<String> _expectLiveInstances;
    private String _zkAddr;
    private ZkClient _zkClient;

    public Builder(String clusterName) {
      _clusterName = clusterName;
    }

    public BestPossibleExternalViewVerifier build() {
      if (_clusterName == null || (_zkAddr == null && _zkClient == null)) {
        throw new IllegalArgumentException("Cluster name or zookeeper info is missing!");
      }

      if (_zkClient != null) {
        return new BestPossibleExternalViewVerifier(_zkClient, _clusterName, _resources, _errStates,
            _expectLiveInstances);
      }
      return new BestPossibleExternalViewVerifier(_zkAddr, _clusterName, _resources, _errStates,
          _expectLiveInstances);
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
      return _zkAddr;
    }

    public Builder setZkAddr(String zkAddr) {
      _zkAddr = zkAddr;
      return this;
    }

    public ZkClient getZkClient() {
      return _zkClient;
    }

    public Builder setZkClient(ZkClient zkClient) {
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

      _clusterDataCache.requireFullRefresh();
      _clusterDataCache.refresh(_accessor);

      Map<String, IdealState> idealStates = new HashMap<>(_clusterDataCache.getIdealStates());

      // filter out all resources that use Task state model
      Iterator<Map.Entry<String, IdealState>> it = idealStates.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, IdealState> pair = it.next();
        if (pair.getValue().getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)) {
          it.remove();
        }
      }

      // verify live instances.
      if (_expectLiveInstances != null && !_expectLiveInstances.isEmpty()) {
        Set<String> actualLiveNodes = _clusterDataCache.getLiveInstances().keySet();
        if (!_expectLiveInstances.equals(actualLiveNodes)) {
          LOG.warn("Live instances are not as expected. Actual live nodes: " + actualLiveNodes.toString());
          return false;
        }
      }

      Map<String, ExternalView> extViews = _accessor.getChildValuesMap(keyBuilder.externalViews());
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
      BestPossibleStateOutput bestPossOutput = calcBestPossState(_clusterDataCache, _resources);
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
        ExternalView extView = extViews.get(resourceName);
        IdealState is = idealStates.get(resourceName);
        if (extView == null) {
          if (is.isExternalViewDisabled()) {
            continue;
          } else {
            LOG.error("externalView for " + resourceName + " is not available");
            return false;
          }
        }

        // step 0: remove empty map and DROPPED state from best possible state
        PartitionStateMap bpStateMap =
            bestPossOutput.getPartitionStateMap(resourceName);

        StateModelDefinition stateModelDef = _clusterDataCache.getStateModelDef(is.getStateModelDefRef());
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
      if (instanceStateMap.isEmpty()) {
        partitionInstanceStateMapIter.remove();
      } else {
        // remove instances with DROPPED and OFFLINE state
        Iterator<Map.Entry<String, String>> insIter = instanceStateMap.entrySet().iterator();
        while (insIter.hasNext()) {
          String state = insIter.next().getValue();
          if (ignoredStates.contains(state)) {
            insIter.remove();
          }
        }
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
  private BestPossibleStateOutput calcBestPossState(ClusterDataCache cache, Set<String> resources)
      throws Exception {
    ClusterEvent event = new ClusterEvent(ClusterEventType.StateVerifier);
    event.addAttribute(AttributeName.ClusterDataCache.name(), cache);

    runStage(event, new ResourceComputationStage());

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

    runStage(event, new CurrentStateComputationStage());
    // TODO: be caution here, should be handled statelessly.
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    return output;
  }

  private void runStage(ClusterEvent event, Stage stage) throws Exception {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    stage.process(event);
    stage.postProcess();
  }

  @Override
  public String toString() {
    String verifierName = getClass().getSimpleName();
    return verifierName + "(" + _clusterName + "@" + _zkClient + "@resources["
       + (_resources != null ? Arrays.toString(_resources.toArray()) : "") + "])";
  }
}
