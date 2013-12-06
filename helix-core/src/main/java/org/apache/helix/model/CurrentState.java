package org.apache.helix.model;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.log4j.Logger;

/**
 * Current states of partitions in a resource for an instance.
 */
public class CurrentState extends HelixProperty {
  private static Logger LOG = Logger.getLogger(CurrentState.class);

  /**
   * Lookup keys for the current state
   */
  public enum CurrentStateProperty {
    SESSION_ID,
    CURRENT_STATE,
    REQUESTED_STATE,
    INFO,
    STATE_MODEL_DEF,
    STATE_MODEL_FACTORY_NAME,
    RESOURCE // ,
             // BUCKET_SIZE
  }

  /**
   * Instantiate a current state with a resource
   * @param resourceName name identifying the resource
   */
  public CurrentState(String resourceName) {
    super(resourceName);
  }

  /**
   * Instantiate a current state with a resource
   * @param resourceId identifier for the resource
   */
  public CurrentState(ResourceId resourceId) {
    super(resourceId.stringify());
  }

  /**
   * Instantiate a current state with a pre-populated ZNRecord
   * @param record a ZNRecord corresponding to the current state
   */
  public CurrentState(ZNRecord record) {
    super(record);
  }

  /**
   * Get the name of the resource
   * @return String resource identifier
   */
  public String getResourceName() {
    return _record.getId();
  }

  /**
   * Get the resource id
   * @return ResourceId
   */
  public ResourceId getResourceId() {
    return ResourceId.from(getResourceName());
  }

  /**
   * Get the partitions on this instance and the state that each partition is currently in.
   * @return (partition, state) pairs
   */
  public Map<String, String> getPartitionStateMap() {
    Map<String, String> map = new HashMap<String, String>();
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    for (String partitionName : mapFields.keySet()) {
      Map<String, String> tempMap = mapFields.get(partitionName);
      if (tempMap != null) {
        map.put(partitionName, tempMap.get(CurrentStateProperty.CURRENT_STATE.toString()));
      }
    }
    return map;
  }

  /**
   * Get the partitions on this instance and the state that each partition is currently in
   * @return (partition id, state) pairs
   */
  public Map<PartitionId, State> getTypedPartitionStateMap() {
    Map<PartitionId, State> map = new HashMap<PartitionId, State>();
    for (String partitionName : _record.getMapFields().keySet()) {
      Map<String, String> stateMap = _record.getMapField(partitionName);
      if (stateMap != null) {
        map.put(PartitionId.from(partitionName),
            State.from(stateMap.get(CurrentStateProperty.CURRENT_STATE.toString())));
      }
    }
    return map;
  }

  /**
   * Get the session that this current state corresponds to
   * @return session identifier
   */
  public SessionId getTypedSessionId() {
    return SessionId.from(getSessionId());
  }

  /**
   * Get the session that this current state corresponds to
   * @return session identifier
   */
  public String getSessionId() {
    return _record.getSimpleField(CurrentStateProperty.SESSION_ID.toString());
  }

  /**
   * Set the session that this current state corresponds to
   * @param sessionId session identifier
   */
  public void setSessionId(SessionId sessionId) {
    setSessionId(sessionId.stringify());
  }

  /**
   * Set the session that this current state corresponds to
   * @param sessionId session identifier
   */
  public void setSessionId(String sessionId) {
    _record.setSimpleField(CurrentStateProperty.SESSION_ID.toString(), sessionId);
  }

  /**
   * Get the state of a partition on this instance
   * @param partitionName the name of the partition
   * @return the state, or null if the partition is not present
   */
  public String getState(String partitionName) {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    Map<String, String> mapField = mapFields.get(partitionName);
    if (mapField != null) {
      return mapField.get(CurrentStateProperty.CURRENT_STATE.toString());
    }
    return null;
  }

  /**
   * Get the state of a partition on this instance
   * @param partitionId partition id
   * @return State
   */
  public State getState(PartitionId partitionId) {
    return State.from(getState(partitionId.stringify()));
  }

  /**
   * Set the state model that the resource follows
   * @param stateModelName an identifier of the state model
   */
  public void setStateModelDefRef(String stateModelName) {
    _record.setSimpleField(CurrentStateProperty.STATE_MODEL_DEF.toString(), stateModelName);
  }

  /**
   * Get the state model that the resource follows
   * @return an identifier of the state model
   */
  public String getStateModelDefRef() {
    return _record.getSimpleField(CurrentStateProperty.STATE_MODEL_DEF.toString());
  }

  /**
   * Set the state model that the resource follows
   * @param stateModelName an identifier of the state model
   */
  public void setStateModelDefId(StateModelDefId stateModelId) {
    _record.setSimpleField(CurrentStateProperty.STATE_MODEL_DEF.toString(),
        stateModelId.stringify());
  }

  /**
   * Get the state model that the resource follows
   * @return an identifier of the state model
   */
  public StateModelDefId getStateModelDefId() {
    return StateModelDefId.from(getStateModelDefRef());
  }

  /**
   * Set the state that a partition is currently in on this instance
   * @param partitionId the id of the partition
   * @param state the state of the partition
   */
  public void setState(PartitionId partitionId, State state) {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    if (mapFields.get(partitionId.stringify()) == null) {
      mapFields.put(partitionId.stringify(), new TreeMap<String, String>());
    }
    mapFields.get(partitionId.stringify()).put(CurrentStateProperty.CURRENT_STATE.toString(),
        state.toString());
  }

  /**
   * Set the state that a partition is currently in on this instance
   * @param partitionName the name of the partition
   * @param state the state of the partition
   */
  public void setState(String partitionName, String state) {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    if (mapFields.get(partitionName) == null) {
      mapFields.put(partitionName, new TreeMap<String, String>());
    }
    mapFields.get(partitionName).put(CurrentStateProperty.CURRENT_STATE.toString(), state);
  }

  /**
   * Set the state model factory
   * @param factoryName the name of the factory
   */
  public void setStateModelFactoryName(String factoryName) {
    _record.setSimpleField(CurrentStateProperty.STATE_MODEL_FACTORY_NAME.toString(), factoryName);
  }

  /**
   * Get the state model factory
   * @return a name that identifies the state model factory
   */
  public String getStateModelFactoryName() {
    return _record.getSimpleField(CurrentStateProperty.STATE_MODEL_FACTORY_NAME.toString());
  }

  // @Override
  // public int getBucketSize()
  // {
  // String bucketSizeStr =
  // _record.getSimpleField(CurrentStateProperty.BUCKET_SIZE.toString());
  // int bucketSize = 0;
  // if (bucketSizeStr != null)
  // {
  // try
  // {
  // bucketSize = Integer.parseInt(bucketSizeStr);
  // }
  // catch (NumberFormatException e)
  // {
  // // OK
  // }
  // }
  // return bucketSize;
  // }
  //
  // @Override
  // public void setBucketSize(int bucketSize)
  // {
  // if (bucketSize > 0)
  // {
  // _record.setSimpleField(CurrentStateProperty.BUCKET_SIZE.toString(), "" + bucketSize);
  // }
  // }

  @Override
  public boolean isValid() {
    if (getStateModelDefRef() == null) {
      LOG.error("Current state does not contain state model ref. id:" + getResourceName());
      return false;
    }
    if (getSessionId() == null) {
      LOG.error("CurrentState does not contain session id, id : " + getResourceName());
      return false;
    }
    return true;
  }

  /**
   * Convert a string map to a concrete partition map
   * @param rawMap map of partition name to state name
   * @return map of partition id to state
   */
  public static Map<PartitionId, State> partitionStateMapFromStringMap(Map<String, String> rawMap) {
    if (rawMap == null) {
      return Collections.emptyMap();
    }
    Map<PartitionId, State> partitionStateMap = new HashMap<PartitionId, State>();
    for (String partitionId : rawMap.keySet()) {
      partitionStateMap.put(PartitionId.from(partitionId), State.from(rawMap.get(partitionId)));
    }
    return partitionStateMap;
  }

  /**
   * Convert a partition map to a string map
   * @param partitionStateMap map of partition id to state
   * @return map of partition name to state name
   */
  public static Map<String, String> stringMapFromPartitionStateMap(
      Map<PartitionId, State> partitionStateMap) {
    if (partitionStateMap == null) {
      return Collections.emptyMap();
    }
    Map<String, String> rawMap = new HashMap<String, String>();
    for (PartitionId partitionId : partitionStateMap.keySet()) {
      rawMap.put(partitionId.stringify(), partitionStateMap.get(partitionId).toString());
    }
    return rawMap;
  }

  /**
   * @param partitionId
   * @return
   */
  public String getInfo(PartitionId partitionId) {
    Map<String, String> mapField = _record.getMapField(partitionId.stringify());
    if (mapField != null) {
      return mapField.get(CurrentStateProperty.INFO.name());
    }
    return null;
  }

  /**
   * @param partitionId
   * @return
   */
  public State getRequestedState(PartitionId partitionId) {
    Map<String, String> mapField = _record.getMapField(partitionId.stringify());
    if (mapField != null) {
      return State.from(mapField.get(CurrentStateProperty.REQUESTED_STATE.name()));
    }
    return null;
  }

  /**
   * @param partitionId
   * @param info
   */
  public void setInfo(PartitionId partitionId, String info) {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    String partitionName = partitionId.stringify();
    if (mapFields.get(partitionName) == null) {
      mapFields.put(partitionName, new TreeMap<String, String>());
    }
    mapFields.get(partitionName).put(CurrentStateProperty.INFO.name(), info);
  }

  /**
   * @param partitionId
   * @param state
   */
  public void setRequestedState(PartitionId partitionId, State state) {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    String partitionName = partitionId.stringify();
    if (mapFields.get(partitionName) == null) {
      mapFields.put(partitionName, new TreeMap<String, String>());
    }
    mapFields.get(partitionName).put(CurrentStateProperty.REQUESTED_STATE.name(), state.toString());
  }
}
