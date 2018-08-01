package org.apache.helix.common.caches;

import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CurrentStateSnapshot extends AbstractDataSnapshot<CurrentState> {
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
      for (PropertyKey propertyKey : _updatedStateKeys) {
        CurrentState prevState = _prevStateMap.get(propertyKey);
        CurrentState curState = _properties.get(propertyKey);

        Map<String, Long> partitionUpdateEndTimes = null;
        for (String partition : curState.getPartitionStateMap().keySet()) {
          long newEndTime = curState.getEndTime(partition);
          if (prevState == null || prevState.getEndTime(partition) < newEndTime) {
            if (partitionUpdateEndTimes == null) {
              partitionUpdateEndTimes = new HashMap<>();
            }
            partitionUpdateEndTimes.put(partition, newEndTime);
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
