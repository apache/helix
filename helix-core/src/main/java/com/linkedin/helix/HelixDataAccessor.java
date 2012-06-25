/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix;

import java.util.List;
import java.util.Map;
/**
 * Interface used to interact with Helix Data Types like IdealState, Config, LiveInstance, Message, ExternalView etc
 * PropertyKey represent the HelixData type. 
 * See {@link Builder} to get more information on building a propertyKey.
 * 
 * @author kgopalak
 *
 */
public interface HelixDataAccessor
{
  /**
   * Create a helix property only if it does not exist.
   * 
   * @param key
   * @param value
   * @return true if creation was successful. False if already exists or if it
   *         failed to create
   */

  boolean createProperty(PropertyKey key, HelixProperty value);

  /**
   * Set a property, overwrite if it exists and creates if not exists. This api
   * assumes the node exists and only tries to update it only if the call fail
   * it will create the node. So there is a performance cost if always ends up
   * creating the node.
   * 
   * @param key
   * @param value
   * @true if the operation was successful
   */
  boolean setProperty(PropertyKey key, HelixProperty value);

  /**
   * Updates a property using newvalue.merge(oldvalue)
   * 
   * @param key
   * @param value
   * @return true if the update was successful
   */
  boolean updateProperty(PropertyKey key, HelixProperty value);

  /**
   * Return the property value, it must be refer to a single Helix Property. i.e
   * PropertyKey.isLeaf() must return true.
   * 
   * @param key
   * @return value, Null if absent or on error
   */
  HelixProperty getProperty(PropertyKey key);

  /**
   * Removes the property
   * 
   * @param key
   * @return true if removal was successful or node does not exist. false if the
   *         node existed and failed to remove it
   */
  boolean removeProperty(PropertyKey key);

  /**
   * Return the child names for a property. PropertyKey needs to refer to a
   * collection like instances, resources. PropertyKey.isLeaf must be false
   * 
   * @param type
   * @return SubPropertyNames
   */
  List<String> getChildNames(PropertyKey key);

  /**
   * Get the child values for a property. PropertyKey needs to refer to just one
   * level above the non leaf. PropertyKey.isCollection must be true.
   * 
   * @param type
   * @return subPropertyValues
   */
  List<HelixProperty> getChildValues(PropertyKey key);

  /**
   * Same as getChildValues except that it converts list into a map using the id
   * of the HelixProperty
   * @param key
   * @return
   */

  Map<String, HelixProperty> getChildValuesMap(PropertyKey key);

}
