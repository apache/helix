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
package org.apache.helix.store;

import java.util.Comparator;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;

/**
 * This property store is similar to a key value store but supports hierarchical
 * structure. It also provides notifications when there is a change in child or
 * parent. Data can be stored child only. Property name cannot end with a "/". 
 * Only the root "/" is an exception. 
 * Property key name is split based on delimiter "/".
 * Suppose we do setProperty("/foo/bar1",val1) and setProperty("/foo/bar2",val2)
 * getProperty("/foo) will return null since no data is stored at /foo
 * getPropertyNames("/foo") will return "bar1" and "bar2" 
 * removeProperty("/foo/bar1") will simply remove the property stored at /foo/bar1
 * 
 * @author kgopalak
 * @param <T>
 */
public interface PropertyStore<T>
{
  /**
   * Set property on key. Override if the property already exists
   * @param key
   * @param value
   * @throws PropertyStoreException
   */
  void setProperty(String key, T value) throws PropertyStoreException;

  /**
   * Get the property on key
   * @param key
   * @return value of the property
   * @throws PropertyStoreException
   */
  T getProperty(String key) throws PropertyStoreException;
  
  /**
   * Get the property and its statistics information
   * @param key
   * @param stat
   * @return value of the property
   * @throws PropertyStoreException
   */
  T getProperty(String key, PropertyStat propertyStat) throws PropertyStoreException;

  /**
   * Removes the property on key
   * @param key
   * @throws PropertyStoreException
   */
  void removeProperty(String key) throws PropertyStoreException;

  /**
   * Get all the child property keys under prefix
   * @param prefix
   * @return
   * @throws PropertyStoreException
   */
  List<String> getPropertyNames(String prefix) throws PropertyStoreException;

  /**
   * Delimiter to split the propertyName
   * @param delimiter
   * @throws PropertyStoreException
   */
  void setPropertyDelimiter(String delimiter) throws PropertyStoreException;

  /**
   * Subscribe for changes in the property property. Key can be a prefix,
   * Multiple callbacks can be invoked. One callback per change is not guaranteed.
   * Some changes might be missed. That's why one has to read the data on every
   * callback.
   * @param prefix
   * @param listener
   * @throws PropertyStoreException
   */
  void subscribeForPropertyChange(String prefix,
      PropertyChangeListener<T> listener) throws PropertyStoreException;

  /**
   * Removes the subscription for the prefix
   * @param prefix
   * @param listener
   * @throws PropertyStoreException
   */
  void unsubscribeForPropertyChange(String prefix,
      PropertyChangeListener<T> listener) throws PropertyStoreException;

  /**
   * Indicates if the implementation supports the feature of storing data in
   * parent
   * @return
   */
  boolean canParentStoreData();

  /**
   * Set property serializer
   * @param serializer
   */
  void setPropertySerializer(PropertySerializer<T> serializer);

  /**
   * create a sub namespace in the property store
   * @return root property path
   */ 
  public void createPropertyNamespace(String prefix) throws PropertyStoreException;
    
  /**
   * Get the root namespace of the property store
   * @return root property path
   */ 
  public String getPropertyRootNamespace();
  
  /**
   * Atomically update property until succeed, updater updates old value to new value
   * Will create key if doesn't exist
   * @param key
   * @param updater
   */
  public void updatePropertyUntilSucceed(String key, DataUpdater<T> updater) 
    throws PropertyStoreException;
  
  /**
   * Atomically update property until succeed, updater updates old value to new value
   * If createIfAbsent is true, will create key if doesn't exist
   * If createIfAbsent is false, will not create key and throw exception
   * @param key
   * @param updater
   * @param createIfAbsent
   * @throws PropertyStoreException
   */
  public void updatePropertyUntilSucceed(String key, DataUpdater<T> updater, boolean createIfAbsent) 
    throws PropertyStoreException;
  
  /**
   * Check if a property exists
   * @param key
   * @return
   */
  public boolean exists(String key);
  
  /**
   * Remove a parent and all its descendants
   * @param prefix
   * @throws PropertyStoreException
   */
  public void removeNamespace(String prefix) throws PropertyStoreException;
  
  /**
   * Update property return true if succeed, false otherwise
   * @param key
   * @param updater
   */
  // public boolean updateProperty(String key, DataUpdater<T> updater);
  
  /**
   * Atomically compare and set property
   * Return true if succeed, false otherwise
   * Create if doesn't exist
   * @param key
   * @param expected value
   * @param updated value
   * @param comparator
   * @param create if absent
   */
  public boolean compareAndSet(String key, T expected, T update, Comparator<T> comparator);
  
  /**
   * Atomically compare and set property
   * Return true if succeed, false otherwise
   * If createIfAbsent is true, create key if doesn't exist
   * If createIfAbsent is false, will not create key and throw exception
   * @param key
   * @param expected
   * @param update
   * @param comparator
   * @param createIfAbsent
   * @return
   */
  public boolean compareAndSet(String key, T expected, T update, Comparator<T> comparator, boolean createIfAbsent);

  /**
   * Start property store
   * @return
   */
  public boolean start();

  /**
   * Stop property store and do clean up
   * @return true
   */
  public boolean stop();

}
