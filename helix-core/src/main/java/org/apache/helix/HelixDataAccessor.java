package org.apache.helix;

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

import java.util.List;
import java.util.Map;

import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Interface used to interact with Helix Data Types like IdealState, Config,
 * LiveInstance, Message, ExternalView etc PropertyKey represent the HelixData
 * type. See {@link PropertyKey.Builder} to get more information on building a propertyKey.
 */
public interface HelixDataAccessor {
  boolean createStateModelDef(StateModelDefinition stateModelDef);

  boolean createControllerMessage(Message message);

  boolean createControllerLeader(LiveInstance leader);

  boolean createPause(PauseSignal pauseSignal);

  boolean createMaintenance(MaintenanceSignal maintenanceSignal);

  /**
   * Set a property, overwrite if it exists and creates if not exists. This api
   * assumes the node exists and only tries to update it only if the call fail
   * it will create the node. So there is a performance cost if always ends up
   * creating the node.
   * @param key
   * @param value
   * @true if the operation was successful
   */
  <T extends HelixProperty> boolean setProperty(PropertyKey key, T value);

  /**
   * Updates a property using newvalue.merge(oldvalue)
   * @param key
   * @param value
   * @return true if the update was successful
   */
  <T extends HelixProperty> boolean updateProperty(PropertyKey key, T value);

  /**
   * Updates a property using specified updater
   * @param key
   * @param updater an update routine for the data to merge in
   * @param value
   * @return true if the update was successful
   */
  <T extends HelixProperty> boolean updateProperty(PropertyKey key, DataUpdater<ZNRecord> updater,
      T value);

  /**
   * Return the property value, it must be refer to a single Helix Property. i.e
   * PropertyKey.isLeaf() must return true.
   * @param key
   * @return value, Null if absent or on error
   */
  <T extends HelixProperty> T getProperty(PropertyKey key);

  /**
   * Return a list of property values, each of which must be refer to a single Helix
   * Property. Property may be bucketized.
   * @param keys
   * @return
   */
  @Deprecated
  <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys);

  /**
   * Return a list of property values, each of which must be refer to a single Helix
   * Property. Property may be bucketized.
   *
   * Value will be null if not does not exist. If the node is failed to read, will throw exception
   * when throwException is set to true.
   *
   * @param keys
   * @param throwException
   * @return
   */
  <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys, boolean throwException);

  /**
   * Removes the property
   * @param key
   * @return true if removal was successful or node does not exist. false if the
   *         node existed and failed to remove it
   */
  boolean removeProperty(PropertyKey key);

  /**
   * Return the metadata (HelixProperty.Stat) of the given property
   * @param key
   * @return
   */
  HelixProperty.Stat getPropertyStat(PropertyKey key);

  /**
   * Return a list of property stats, each of which must refer to a single Helix property.
   * @param keys
   * @return
   */
  List<HelixProperty.Stat> getPropertyStats(List<PropertyKey> keys);

  /**
   * Return the child names for a property. PropertyKey needs to refer to a
   * collection like instances, resources. PropertyKey.isLeaf must be false
   * @param key
   * @return SubPropertyNames
   */
  List<String> getChildNames(PropertyKey key);

  /**
   * Get the child values for a property. PropertyKey needs to refer to just one
   * level above the non leaf. PropertyKey.isCollection must be true.
   * @param key
   * @return subPropertyValues
   */
  @Deprecated
  <T extends HelixProperty> List<T> getChildValues(PropertyKey key);

  /**
   * Get the child values for a property. PropertyKey needs to refer to just one
   * level above the non leaf. PropertyKey.isCollection must be true.
   *
   * Value will be null if not does not exist. If the node is failed to read, will throw exception
   * when throwException is set to true.
   * @param key
   * @param throwException
   * @return subPropertyValues
   */
  <T extends HelixProperty> List<T> getChildValues(PropertyKey key, boolean throwException);

  /**
   * Same as getChildValues except that it converts list into a map using the id
   * of the HelixProperty
   * @param key
   * @return a map of property identifiers to typed properties
   */
  @Deprecated
  <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key);

  /**
   * Same as getChildValues except that it converts list into a map using the id
   * of the HelixProperty
   *
   * Value will be null if not does not exist. If the node is failed to read, will throw exception
   * when throwException is set to true.
   * @param key
   * @param throwException
   * @return a map of property identifiers to typed properties
   */

  <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key,
      boolean throwException);

  /**
   * Adds multiple children to a parent.
   * @param keys
   * @param children
   * @return array where true means the child was added and false means it was not
   */
  <T extends HelixProperty> boolean[] createChildren(List<PropertyKey> keys, List<T> children);

  /**
   * Sets multiple children under one parent
   * @param keys
   * @param children
   * @return array where true means the child was set and false means it was not
   */
  <T extends HelixProperty> boolean[] setChildren(List<PropertyKey> keys, List<T> children);

  /**
   * Updates multiple children under one parent
   * TODO: change to use property-keys instead of paths
   * @param paths
   * @param updaters
   * @return array where true means the child was updated and false means it was not
   */
  <T extends HelixProperty> boolean[] updateChildren(List<String> paths,
      List<DataUpdater<ZNRecord>> updaters, int options);

  /**
   * Get key builder for the accessor
   * @return instantiated PropertyKey.Builder
   */
  PropertyKey.Builder keyBuilder();

  /**
   * Get underlying base data accessor
   * @return a data accessor that can process ZNRecord objects
   */
  BaseDataAccessor<ZNRecord> getBaseDataAccessor();
}
