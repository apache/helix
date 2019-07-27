package org.apache.helix.common.caches;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.MockAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link CurrentStateSnapshot}
 */
public class TestCurrentStateSnapshot {

  // This test makes sure that currentStateEndTimes calculation would record correct partition replica.
  // Specifically, if a replicate has not endTime field set, we should not put an entry into currentStateEndTime
  // calculation. Otherwise, we see huge statePropagation latency of 1.4Tms.
  @Test(description = "test getNewCurrentStateEndTimes")
  public void testGetNewCurrentStateEndTimes() {
    String instance1 = "instance1";
    String session1 = "session1";
    String resource1 = "resource1";

    String partition1 = "partition1";
    String partition2 = "partition2";

    PropertyKey key = new PropertyKey.Builder("cluster").currentState(instance1, session1, resource1);

    CurrentState nxtState = new CurrentState(resource1);
    // partition 1, expect to record in endTimesMap
    nxtState.setState(partition1, "SLAVE");
    nxtState.setEndTime(partition1, 200);
    // partition 2, expect to not record in endTimeMap. This is fixing current 1.4T observed timestamp issue
    nxtState.setState(partition2, "MASTER");

    Map<PropertyKey, CurrentState> currentStateMap = new HashMap<>();
    Map<PropertyKey, CurrentState> nextStateMap = new HashMap<>();
    nextStateMap.put(key, nxtState);

    Set<PropertyKey> updateKeys = new HashSet<>();
    updateKeys.add(key);

    CurrentStateSnapshot snapshot = new CurrentStateSnapshot(nextStateMap, currentStateMap, updateKeys);

    Map<PropertyKey, Map<String, Long>> endTimesMap = snapshot.getNewCurrentStateEndTimes();

    Assert.assertEquals(endTimesMap.size(), 1);
    Assert.assertTrue(endTimesMap.get(key).get(partition1) == 200);
  }

  // This test makes sure that all the changed current State is reflected in newCurrentStateEndTimes calculation.
  // Previously, we have bugs that all newly created current state would be reflected in newCurrentStateEndTimes
  // calculation.
  @Test(description = "testRefreshCurrentStateCache")
  public void testRefreshCurrentStateCache() {
    String instanceName = "instance1";
    long instanceSession = 12345;
    String resourceName = "resource";
    String partitionName = "resource_partition1";

    MockAccessor accessor = new MockAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // construct liveInstance
    ZNRecord record = new ZNRecord(instanceName);
    record.setEphemeralOwner(instanceSession);
    LiveInstance instance = new LiveInstance(record);

    boolean retVal = accessor.setProperty(keyBuilder.liveInstance(instanceName), instance);
    Assert.assertTrue(retVal);

    // construct currentstate
    CurrentState originState = new CurrentState(resourceName);
    originState.setEndTime(partitionName, 100);

    CurrentState currentState = new CurrentState(resourceName);
    currentState.setEndTime(partitionName, 300);
    retVal = accessor.setProperty(keyBuilder.currentState(instanceName, instance.getEphemeralOwner(), resourceName),
        originState);
    Assert.assertTrue(retVal);

    CurrentStateCache cache = new CurrentStateCache("cluster");

    Map<String, LiveInstance> liveInstanceMap = new HashMap<>();
    liveInstanceMap.put(instanceName, instance);

    retVal = cache.refresh(accessor, liveInstanceMap);
    Assert.assertTrue(retVal);

    retVal = accessor.setProperty(keyBuilder.currentState(instanceName, instance.getEphemeralOwner(), resourceName),
        currentState);
    Assert.assertTrue(retVal);

    retVal = cache.refresh(accessor, liveInstanceMap);
    Assert.assertTrue(retVal);

    CurrentStateSnapshot snapshot = cache.getSnapshot();

    Map<PropertyKey, Map<String, Long>> endTimesMap = snapshot.getNewCurrentStateEndTimes();

    Assert.assertEquals(endTimesMap.size(), 1);
    // note, without this fix, the endTimesMap would be size zero.
    Assert.assertTrue(endTimesMap.get(keyBuilder.currentState(instanceName, instance.getEphemeralOwner(), resourceName))
        .get(partitionName) == 300);
  }
}
