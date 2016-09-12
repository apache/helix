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
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.apache.log4j.Logger;

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
  private static Logger LOG = Logger.getLogger(BestPossibleExternalViewVerifier.class);

  private final Map<String, Map<String, String>> _errStates;
  private final Set<String> _resources;
  private final Set<String> _expectLiveInstances;

  public BestPossibleExternalViewVerifier(String zkAddr, String clusterName, Set<String> resources,
      Map<String, Map<String, String>> errStates, Set<String> expectLiveInstances) {
    super(zkAddr, clusterName);
    _errStates = errStates;
    _resources = resources;
    _expectLiveInstances = expectLiveInstances;
  }

  public BestPossibleExternalViewVerifier(ZkClient zkClient, String clusterName,
      Set<String> resources, Map<String, Map<String, String>> errStates,
      Set<String> expectLiveInstances) {
    super(zkClient, clusterName);
    _errStates = errStates;
    _resources = resources;
    _expectLiveInstances = expectLiveInstances;
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
  protected boolean verifyState() {
    try {
      PropertyKey.Builder keyBuilder = _accessor.keyBuilder();
      // read cluster once and do verification
      ClusterDataCache cache = new ClusterDataCache();
      cache.refresh(_accessor);

      Map<String, IdealState> idealStates = cache.getIdealStates();
      if (idealStates == null) {
        // ideal state is null because ideal state is dropped
        idealStates = Collections.emptyMap();
      }

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
        Set<String> actualLiveNodes = cache.getLiveInstances().keySet();
        if (!_expectLiveInstances.equals(actualLiveNodes)) {
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
          idealStates.put(resource, new IdealState(resource));
        }
      }

      // calculate best possible state
      BestPossibleStateOutput bestPossOutput = calcBestPossState(cache);
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
            LOG.debug("externalView for " + resourceName + " is not available");
            return false;
          }
        }

        // step 0: remove empty map and DROPPED state from best possible state
        Map<Partition, Map<String, String>> bpStateMap =
            bestPossOutput.getResourceMap(resourceName);

        boolean result = verifyExternalView(is, extView, bpStateMap);
        if (!result) {
          LOG.debug("verifyExternalView fails! ExternalView: " + extView + " BestPossibleState: "
              + bpStateMap);
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      LOG.error("exception in verification", e);
      return false;
    }
  }

  private boolean verifyExternalView(IdealState idealState, ExternalView externalView,
      Map<Partition, Map<String, String>> bestPossibleState) {

    StateModelDefinition stateModelDef =
        BuiltInStateModelDefinitions.valueOf(idealState.getStateModelDefRef())
            .getStateModelDefinition();
    Set<String> ignoreStaes = new HashSet<String>(
        Arrays.asList(stateModelDef.getInitialState(), HelixDefinedState.DROPPED.toString()));

    Map<String, Map<String, String>> bestPossibleStateMap =
        convertBestPossibleState(bestPossibleState);
    removeEntryWithIgnoredStates(bestPossibleStateMap.entrySet().iterator(), ignoreStaes);

    Map<String, Map<String, String>> externalViewMap = externalView.getRecord().getMapFields();
    removeEntryWithIgnoredStates(externalViewMap.entrySet().iterator(), ignoreStaes);

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
      Map<Partition, Map<String, String>> bestPossibleState) {
    Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
    for (Partition partition : bestPossibleState.keySet()) {
      result.put(partition.getPartitionName(), bestPossibleState.get(partition));
    }
    return result;
  }

  /**
   * calculate the best possible state note that DROPPED states are not checked since when
   * kick off the BestPossibleStateCalcStage we are providing an empty current state map
   *
   * @param cache
   * @return
   * @throws Exception
   */
  private BestPossibleStateOutput calcBestPossState(ClusterDataCache cache) throws Exception {
    ClusterEvent event = new ClusterEvent("sampleEvent");
    event.addAttribute("ClusterDataCache", cache);

    runStage(event, new ResourceComputationStage());
    runStage(event, new CurrentStateComputationStage());

    // TODO: be caution here, should be handled statelessly.
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());

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
