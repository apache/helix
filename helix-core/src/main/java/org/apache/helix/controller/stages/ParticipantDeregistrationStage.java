package org.apache.helix.controller.stages;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.helix.util.RebalanceUtil.scheduleOnDemandPipeline;


public class ParticipantDeregistrationStage extends AbstractAsyncBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(ParticipantDeregistrationStage.class);

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.ParticipantDeregistrationWorker;
  }

  @Override
  public void execute(ClusterEvent event) throws Exception {
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    ResourceControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    ClusterConfig clusterConfig = cache.getClusterConfig();
    if (clusterConfig == null || !clusterConfig.isParticipantDeregistrationEnabled()) {
      LOG.debug("Cluster config is null or participant deregistration is not enabled. "
          + "Skipping participant deregistration.");
      return;
    }

    Map<String, Long> offlineTimeMap = cache.getInstanceOfflineTimeMap();
    long deregisterDelay = clusterConfig.getParticipantDeregistrationTimeout();
    long stageStartTime = System.currentTimeMillis();
    Set<String> participantsToDeregister = new HashSet<>();
    long nextDeregisterTime = -1;


    for (Map.Entry<String, Long> entry : offlineTimeMap.entrySet()) {
      String instanceName = entry.getKey();
      Long offlineTime = entry.getValue();
      long deregisterTime = offlineTime + deregisterDelay;

      // Skip if instance is still online
      if (cache.getLiveInstances().containsKey(instanceName)) {
        continue;
      }

      // If deregister time is in the past, deregister the instance
      if (deregisterTime <= stageStartTime) {
        participantsToDeregister.add(instanceName);
      } else {
        // Otherwise, find the next earliest deregister time
        nextDeregisterTime = nextDeregisterTime == -1 ? deregisterTime : Math.min(nextDeregisterTime, deregisterTime);
      }
    }

    deregisterParticipants(manager, cache, participantsToDeregister);

    // Schedule the next deregister task
    if (nextDeregisterTime != -1) {
      long delay = Math.max(nextDeregisterTime - System.currentTimeMillis(), 0);
      scheduleOnDemandPipeline(manager.getClusterName(), delay);
    }
  }

  private void deregisterParticipants(HelixManager manager, ResourceControllerDataProvider cache,
      Set<String> instancesToDeregister) {
    Set<String> successfullyDeregisteredInstances = new HashSet<>();

    if (manager == null || !manager.isConnected() || cache == null || instancesToDeregister == null) {
      LOG.warn("ParticipantDeregistrationStage failed due to HelixManager being null or not connected!");
      return;
    }

    if (instancesToDeregister.isEmpty()) {
      LOG.debug("There are no instances to deregister from cluster {}", cache.getClusterName());
      return;
    }

    // Perform safety checks before deregistering the instances
    for (String instanceName : instancesToDeregister) {
      InstanceConfig instanceConfig = cache.getInstanceConfigMap().get(instanceName);
      if (instanceConfig == null) {
        LOG.debug("Instance config is null for instance {}, skip deregistering the instance", instanceName);
        continue;
      }

      try {
        manager.getClusterManagmentTool().dropInstance(cache.getClusterName(), instanceConfig);
        successfullyDeregisteredInstances.add(instanceName);
        LOG.info("Successfully deregistered instance {} from cluster {}", instanceName, cache.getClusterName());
      } catch (HelixException e) {
        LOG.warn("Failed to deregister instance {} from cluster {}", instanceName, cache.getClusterName(), e);
      }
    }
  }
}
