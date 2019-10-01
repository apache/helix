package org.apache.helix.examples.rebalancer.simulator.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.examples.rebalancer.simulator.AbstractSimulator;
import org.apache.helix.examples.rebalancer.simulator.operations.AddNode;
import org.apache.helix.examples.rebalancer.simulator.operations.AddResource;
import org.apache.helix.examples.rebalancer.simulator.operations.EnableMaintenanceMode;
import org.apache.helix.examples.rebalancer.simulator.operations.EnableNode;
import org.apache.helix.examples.rebalancer.simulator.operations.ModifyResource;
import org.apache.helix.examples.rebalancer.simulator.operations.Operation;
import org.apache.helix.examples.rebalancer.simulator.operations.RebootNode;
import org.apache.helix.examples.rebalancer.simulator.operations.RemoveNode;
import org.apache.helix.examples.rebalancer.simulator.operations.RemoveResource;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;

public class ExampleScripts {
  public static List<Operation> showCaseOperations(int partitionCount, int replicaCount,
      Map<String, Integer> defaultUsage, String rebalanaceClass, String rebalanceStrategy,
      AbstractSimulator simulator) {
    String targetResourceName = "newResource";
    List<Operation> operations = new ArrayList<>();
    operations.add(
        new AddResource(targetResourceName, BuiltInStateModelDefinitions.OnlineOffline.name(),
            defaultUsage, partitionCount, replicaCount, rebalanaceClass, rebalanceStrategy));
    String targetNode = "newInstance0_12000";
    operations.add(new EnableNode(targetNode, false));
    operations.add(new EnableNode(targetNode, true));

    IdealState newIs = new IdealState(targetResourceName);
    newIs.setReplicas("1");
    operations.add(new ModifyResource(null, newIs));

    operations.add(new RebootNode(targetNode, simulator));
    operations.add(new RemoveNode(targetNode, simulator));
    operations.add(new RemoveResource(targetResourceName));

    return operations;
  }

  public static List<Operation> rollingUpgrade(Set<String> faultZones,
      AbstractSimulator simulator) {
    List<Operation> operations = new ArrayList<>();
    for (String zone : faultZones) {
      // disable
      operations.add(new Operation() {
        @Override
        public boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName) {
          List<String> nodes = simulator.getNodeToZoneMap().entrySet().stream()
              .filter(entry -> entry.getValue().equals(zone)).map(entry -> entry.getKey())
              .collect(Collectors.toList());
          for (String node : nodes) {
            InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, node);
            instanceConfig.setInstanceEnabled(false);
            admin.setInstanceConfig(clusterName, node, instanceConfig);
          }
          return true;
        }

        @Override
        public String getDescription() {
          return "Disable all nodes in zone: " + zone;
        }
      });
      // reset and re-enable
      operations.add(new Operation() {
        @Override
        public boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName) {
          List<String> nodes = simulator.getNodeToZoneMap().entrySet().stream()
              .filter(entry -> entry.getValue().equals(zone)).map(entry -> entry.getKey())
              .collect(Collectors.toList());
          for (String node : nodes) {
            try {
              simulator.resetProcess(node);
            } catch (Exception e) {
              return false;
            }
            InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, node);
            instanceConfig.setInstanceEnabled(true);
            admin.setInstanceConfig(clusterName, node, instanceConfig);
          }
          return true;
        }

        @Override
        public String getDescription() {
          return "Reset and then re-enable all nodes in zone: " + zone;
        }
      });
    }
    return operations;
  }

  public static List<Operation> migrateToWagedRebalancer() {
    List<Operation> operations = new ArrayList<>();
    operations.add(new EnableMaintenanceMode(true));
    operations.add(new Operation() {
      @Override
      public boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName) {
        for (String resource : admin.getResourcesInCluster(clusterName)) {
          IdealState is = admin.getResourceIdealState(clusterName, resource);
          is.setRebalancerClassName(WagedRebalancer.class.getName());
          admin.setResourceIdealState(clusterName, resource, is);
        }
        return true;
      }

      @Override
      public String getDescription() {
        return "Use WAGED rebalancer for all resources.";
      }
    });
    operations.add(new EnableMaintenanceMode(false));
    return operations;
  }

  public static List<Operation> expandFaultZones(int newNodeCountForEachZone,
      Map<String, Integer> capacityMap, Set<String> faultZones, String topology,
      String faultZoneType, AbstractSimulator simulator) {
    List<Operation> operations = new ArrayList<>();
    operations.add(new EnableMaintenanceMode(true));
    for (String faultZone : faultZones) {
      for (int i = 0; i < newNodeCountForEachZone; i++) {
        String nodeName = "expansionNodes_" + i + "_" + faultZone;
        StringBuilder domainStrBuilder = new StringBuilder();
        String[] keys = topology.split("/");
        int j = 0;
        for (; j < keys.length - 1; j++) {
          String key = keys[j];
          if (!key.isEmpty()) {
            if (key.equals(faultZoneType)) {
              domainStrBuilder.append(key + "=" + faultZone + ",");
            } else {
              domainStrBuilder.append(key + "=DEFAULT,");
            }
          }
        }
        domainStrBuilder.append(keys[j] + "=" + nodeName);
        operations.add(new AddNode(nodeName, i, capacityMap, faultZone, domainStrBuilder.toString(),
            simulator));
      }
    }
    operations.add(new EnableMaintenanceMode(false));
    return operations;
  }

  public static List<Operation> shrinkFaultZones(int removingNodeForEachZone,
      Set<String> faultZones, AbstractSimulator simulator) {
    List<Operation> operations = new ArrayList<>();
    operations.add(new EnableMaintenanceMode(true));
    for (String faultZone : faultZones) {
      operations.add(new Operation() {
        @Override
        public boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName) {
          List<String> nodes = simulator.getNodeToZoneMap().entrySet().stream()
              .filter(entry -> entry.getValue().equals(faultZone)).map(entry -> entry.getKey())
              .collect(Collectors.toList());
          for (int i = 0; i < removingNodeForEachZone; i++) {
            String instanceName = nodes.get(i);
            simulator.stopProcess(instanceName);
            admin.dropInstance(clusterName, admin.getInstanceConfig(clusterName, instanceName));
            try {
              admin.getInstanceConfig(clusterName, instanceName);
              return false;
            } catch (HelixException ex) {
              simulator.getNodeToZoneMap().remove(instanceName);
            }
          }
          return true;
        }

        @Override
        public String getDescription() {
          return String
              .format("Remove %d nodes from fault zone %s", removingNodeForEachZone, faultZone);
        }
      });
    }
    operations.add(new EnableMaintenanceMode(false));
    return operations;
  }
}
