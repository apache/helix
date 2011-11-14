package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.store.PropertyStore;

public interface ClusterDataAccessor
{
  public enum Type
  {
    CLUSTER, INSTANCE, CONTROLLER;
  }

  /**
   * Set a property, overwrite if it exists and creates if not exists
   *
   * @param type
   * @param value
   * @param keys
   * @true if the operation was successful
   */
   boolean setProperty(PropertyType type, ZNRecord value, String... keys);

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
   boolean updateProperty(PropertyType type, ZNRecord value,
      String... keys);

  /**
   * Return the property value, it must be a leaf
   *
   * @param type
   * @param keys
   *          one or more keys used to get the path of znode
   * @return value, Null if absent or on error
   */
   ZNRecord getProperty(PropertyType type, String... keys);

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


  PropertyStore<ZNRecord> getStore();

  public enum InstanceConfigProperty
  {
    HOST, PORT, ENABLED
  }

  public enum IdealStateConfigProperty
  {
    AUTO, CUSTOMIZED
  }

}
