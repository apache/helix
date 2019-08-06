package org.apache.helix.common.caches;

import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CurrentStateSnapshot extends AbstractDataSnapshot<CurrentState> {
  private static final Logger LOG = LoggerFactory.getLogger(CurrentStateSnapshot.class.getName());

  private Set<PropertyKey> _updatedStateKeys = null;
  private Map<PropertyKey, CurrentState> _prevStateMap = null;

  public CurrentStateSnapshot(final Map<PropertyKey, CurrentState> currentStateMap) {
    super(currentStateMap);
  }

  public CurrentStateSnapshot(final Map<PropertyKey, CurrentState> currentStateMap,
      final Map<PropertyKey, CurrentState> prevStateMap, final Set<PropertyKey> updatedStateKeys) {
    this(currentStateMap);
    _updatedStateKeys = Collections.unmodifiableSet(new HashSet<>(updatedStateKeys));
    _prevStateMap = Collections.unmodifiableMap(new HashMap<>(prevStateMap));
  }

  /**
   * Return the end times of all recent changed current states update.
   */
  public Map<PropertyKey, Map<String, Long>> getNewCurrentStateEndTimes() {
    Map<PropertyKey, Map<String, Long>> endTimeMap = new HashMap<>();
    if (_updatedStateKeys != null && _prevStateMap != null) {
      // Note if the prev state map is empty, this is the first time refresh.
      // So the update is not considered as "recent" change.
      int driftCnt = 0; // clock drift count for comparing timestamp
      for (PropertyKey propertyKey : _updatedStateKeys) {
        CurrentState prevState = _prevStateMap.get(propertyKey);
        CurrentState curState = _properties.get(propertyKey);

        Map<String, Long> partitionUpdateEndTimes = null;
        for (String partition : curState.getPartitionStateMap().keySet()) {
          long newEndTime = curState.getEndTime(partition);
          // if prevState is null, and newEndTime is -1, we should not record -1 in endTimeMap; otherwise,
          // statePropagation latency calculation in RoutingTableProvider would spit out extremely large metrics.
          if ((prevState == null || prevState.getEndTime(partition) < newEndTime) && newEndTime != -1) {
            if (partitionUpdateEndTimes == null) {
              partitionUpdateEndTimes = new HashMap<>();
            }
            partitionUpdateEndTimes.put(partition, newEndTime);
          } else if (prevState != null && prevState.getEndTime(partition) > newEndTime) {
            // This can happen due to clock drift.
            // updatedStateKeys is the path to resource in an instance config.
            // Thus, the space of inner loop is Sigma{replica(i) * partition(i)}; i over all resources in the cluster
            // This space can be large. In order not to print too many lines, we print first warning for the first case.
            // If clock drift turns out to be common, we can consider print out more logs, or expose an metric.
            if (driftCnt < 1) {
              LOG.warn(
                  "clock drift. partition:" + partition + " curState:" + curState.getState(partition) + " prevState: "
                      + prevState.getState(partition));
            }
            driftCnt++;
          }
        }

        if (partitionUpdateEndTimes != null) {
          endTimeMap.put(propertyKey, partitionUpdateEndTimes);
        }
      }
    }
    return endTimeMap;
  }
}
