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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
/**
 * use {@link HelixDataAccessor}.
 */
@Deprecated
public interface DataAccessor
{

  /**
   * Set a property, overwrite if it exists and creates if not exists
   * 
   * @param type
   * @param value
   * @param keys
   * @true if the operation was successful
   */
  boolean setProperty(PropertyType type, ZNRecord value, String... keys);

  boolean setProperty(PropertyType type, HelixProperty value, String... keys);

  /**
   * Updates a property, either overwrite or merge based on the
   * propertyType.mergeOnUpdate, fails to update if
   * propertyType.updateOnlyOnExists and does not exist
   * 
   * @param type
   * @param value
   * @param keys
   * @return true if the update was successful
   */
  boolean updateProperty(PropertyType type, ZNRecord value, String... keys);

  boolean updateProperty(PropertyType type, HelixProperty value, String... keys);

  /**
   * Return the property value, it must be a leaf
   * 
   * @param type
   * @param keys
   *          one or more keys used to get the path of znode
   * @return value, Null if absent or on error
   */
  ZNRecord getProperty(PropertyType type, String... keys);

  <T extends HelixProperty> T getProperty(Class<T> clazz, PropertyType type, String... keys);

  /**
   * Removes the property
   * 
   * @param type
   * @param keys
   * @return
   */
  boolean removeProperty(PropertyType type, String... keys);

  /**
   * Return the child names of the property
   * 
   * @param type
   * @param keys
   * @return SubPropertyNames
   */
  List<String> getChildNames(PropertyType type, String... keys);

  /**
   * 
   * @param type
   * @param keys
   *          must point to parent of leaf znodes
   * @return subPropertyValues
   */
  List<ZNRecord> getChildValues(PropertyType type, String... keys);

  <T extends HelixProperty> List<T> getChildValues(Class<T> clazz, PropertyType type,
      String... keys);

  <T extends HelixProperty> Map<String, T> getChildValuesMap(Class<T> clazz, PropertyType type,
      String... keys);
}
