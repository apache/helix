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
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ParticipantHistory;
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
    ClusterConfig clusterConfig = manager.getConfigAccessor().getClusterConfig(manager.getClusterName());
    if (clusterConfig == null || !clusterConfig.isParticipantDeregistrationEnabled()) {
      LOG.info("Cluster config is null or participant deregistration is not enabled. "
          + "Skipping participant deregistration.");
      return;
    }

    ResourceControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    Map<String, Long> offlineTimeMap = cache.getInstanceOfflineTimeMap();
    long deregisterDelay = clusterConfig.getParticipantDeregistrationTimeout();
    long stageStartTime = System.currentTimeMillis();
    Set<String> participantsToDeregister = new HashSet<>();
    long earliestDeregisterTime = Long.MAX_VALUE;


    for (Map.Entry<String, Long> entry : offlineTimeMap.entrySet()) {
      String instanceName = entry.getKey();
      Long offlineTime = entry.getValue();
      long deregisterTime = offlineTime + deregisterDelay;

      // Skip if instance is still online
      if (offlineTime == ParticipantHistory.ONLINE) {
        continue;
      }

      // If deregister time is in the past, deregister the instance
      if (deregisterTime <= stageStartTime) {
        participantsToDeregister.add(instanceName);
      } else {
        // Otherwise, find the next earliest deregister time
        if (deregisterTime < earliestDeregisterTime) {
          earliestDeregisterTime = deregisterTime;
        }
      }
    }

    if (!participantsToDeregister.isEmpty()) {
      Set<String> successfullyDeregisteredParticipants =
        deregisterParticipants(manager, cache, participantsToDeregister);
      if (!successfullyDeregisteredParticipants.isEmpty()) {
        LOG.info("Successfully deregistered {} participants from cluster {}",
              successfullyDeregisteredParticipants.size(), cache.getClusterName());
      }
    }
    // Schedule the next deregister task
    if (earliestDeregisterTime != Long.MAX_VALUE) {
      long delay = earliestDeregisterTime - stageStartTime;
      scheduleOnDemandPipeline(manager.getClusterName(), delay);
    }
  }

  private Set<String> deregisterParticipants(HelixManager manager, ResourceControllerDataProvider cache,
      Set<String> instancesToDeregister) {
    Set<String> successfullyDeregisteredInstances = new HashSet<>();

    if (manager == null || !manager.isConnected() || cache == null || instancesToDeregister == null) {
      LOG.info("ParticipantDeregistrationStage failed due to HelixManager being null or not connected!");
      return successfullyDeregisteredInstances;
    }

    // Perform safety checks before deregistering the instances
    for (String instanceName : instancesToDeregister) {
      InstanceConfig instanceConfig = cache.getInstanceConfigMap().get(instanceName);
      LiveInstance liveInstance = cache.getLiveInstances().get(instanceName);

      if (instanceConfig == null) {
        LOG.debug("Instance config is null for instance {}, skip deregistering the instance", instanceName);
        continue;
      }

      if (liveInstance != null) {
        LOG.debug("Instance {} is still alive, skip deregistering the instance", instanceName);
        continue;
      }

      try {
        manager.getClusterManagmentTool().dropInstance(cache.getClusterName(), instanceConfig);
        successfullyDeregisteredInstances.add(instanceName);
      } catch (HelixException e) {
        LOG.error("Failed to deregister instance {} from cluster {}", instanceName, cache.getClusterName(), e);
      }
    }

    return successfullyDeregisteredInstances;
  }
}
