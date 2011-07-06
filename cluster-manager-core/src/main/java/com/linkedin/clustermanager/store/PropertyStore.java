package com.linkedin.clustermanager.store;

import java.util.List;

import org.I0Itec.zkclient.DataUpdater;

/**
 * This property store is similar to a key value but supports hierarchical
 * structure. It also provides notifications when there is a change in child or
 * parent Data can be stored at both parent and child based on what
 * canParentStoreData It might be difficult for some implementation to support
 * this feature. Property name cannot end with a "/". Only the root "/" is an
 * exception Property key name is split based on delimiter.
 * setProperty("/foo/bar1",val1); setProperty("/foo/bar2",val2);
 * getProperty("/foo) will return null since no data is stored at /foo
 * getProperties("/foo"); will return "bar1" and "bar2" removeProperty will
 * simply remove the property
 * 
 * @author kgopalak
 * @param <T>
 */
public interface PropertyStore<T>
{
  /**
   * Override if the property already exists
   * 
   * @param key
   * @param value
   * @throws PropertyStoreException
   */
  void setProperty(String key, T value) throws PropertyStoreException;

  /**
   * get the property
   * 
   * @param key
   * @return value of the property
   * @throws PropertyStoreException
   */
  // List<T> getProperty(String key) throws PropertyStoreException;
  T getProperty(String key) throws PropertyStoreException;
  
  /**
   * get the property
   * 
   * @param key
   * @param stat
   * @return value of the property
   * @throws PropertyStoreException
   */
  // List<T> getProperty(String key, List<PropertyStat> propertyStatList) throws PropertyStoreException;
  T getProperty(String key, PropertyStat propertyStatList) throws PropertyStoreException;

  /**
   * removes the property
   * 
   * @param key
   * @throws PropertyStoreException
   */
  void removeProperty(String key) throws PropertyStoreException;

  /**
   * @param prefix
   * @return
   * @throws PropertyStoreException
   */
  List<String> getPropertyNames(String prefix) throws PropertyStoreException;

  /**
   * Delimiter to split the propertyName
   * 
   * @param delimiter
   * @throws PropertyStoreException
   */
  void setPropertyDelimiter(String delimiter) throws PropertyStoreException;

  /**
   * subscribe for changes in the property property can be a child or parent,
   * Multiple callbacks can be invoked.One callback per change is not guaranteed
   * Some changes might be missed. Thats why one has to read the data on every
   * callback.
   * 
   * @param prefix
   * @param listener
   * @throws PropertyStoreException
   */
  void subscribeForPropertyChange(String prefix,
      PropertyChangeListener<T> listener) throws PropertyStoreException;

  /**
   * Removes the listener for the prefix
   * 
   * @param prefix
   * @param listener
   * @throws PropertyStoreException
   */
  void unsubscribeForPropertyChange(String prefix,
      PropertyChangeListener<T> listener) throws PropertyStoreException;

  /**
   * Indicates if the implementation supports the feature of storing data in
   * parent
   * 
   * @return
   */
  boolean canParentStoreData();

  /**
   * Set the property serializer
   * 
   * @param serializer
   */
  void setPropertySerializer(PropertySerializer serializer);

  /**
   * create a sub namespace in the property store
   * 
   * @return root property path
   */ 
  public void createPropertyNamespace(String prefix);
    
  /**
   * return the root namespace of the property store
   * 
   * @return root property path
   */ 
  public String getPropertyRootNamespace();
  
  /**
   * update property until succeed, updater updates old_value to new_value
   * 
   * @param key
   * @param updater
   */
  public void updatePropertyUntilSucceed(String key, DataUpdater<T> updater);
  
  /**
   * update property return true if succeed, false otherwise
   * 
   * @param key
   * @param updater
   */
  public boolean updateProperty(String key, DataUpdater<T> updater);

}
