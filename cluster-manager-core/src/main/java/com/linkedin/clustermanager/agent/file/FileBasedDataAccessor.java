package com.linkedin.clustermanager.agent.file;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.store.file.FilePropertyStore;

public class FileBasedDataAccessor implements ClusterDataAccessor
{
  private static Logger logger = Logger.getLogger(FileBasedDataAccessor.class);
  private final FilePropertyStore<ZNRecord> _store;
  private final String _clusterName;  
  private final ReadWriteLock _readWriteLock = new ReentrantReadWriteLock();
  
  public FileBasedDataAccessor(FilePropertyStore<ZNRecord> store, String clusterName)
  {
    _store = store;
    _clusterName = clusterName;
  }
  
  /*
  @Override
  public void setClusterProperty(PropertyType clusterProperty,
      String key, ZNRecord value)
  {
    
    // String path = CMUtil.getPropertyPath(_clusterName, clusterProperty);
    // path = path + "/" + key;String 
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    
    try
    {
      _readWriteLock.writeLock().lock();
      _store.setProperty(path, value);
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to set cluster property clusterName: " + _clusterName + 
                " type:" + type +
                " keys:" + keys + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }
  }
  */
  
  /*
  @Override
  public void updateClusterProperty(PropertyType type, String key, ZNRecord value)
  {
    throw new UnsupportedOperationException(
      "updateClusterProperty() is NOT supported by FileDataAccessor");

  }
  */

  /*
  @Override
  // public ZNRecord getProperty(PropertyType type, String key)
  public ZNRecord getProperty(PropertyType type, String... keys)
  {
    // String path = CMUtil.getPropertyPath(_clusterName, type);
    // path = path + "/" + key;
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);

    try
    {
      _readWriteLock.readLock().lock();
      return _store.getProperty(path);
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to get cluster property clusterName: " + _clusterName + 
                " type:" + type +
                " keys:" + keys + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
    return null;
  }
  */
  
  /*
  @Override
  public List<ZNRecord> getPropertyList(PropertyType clusterProperty)
  {
    String path = CMUtil.getPropertyPath(_clusterName, clusterProperty);
    return getChildRecords(path);    
  }

  @Override
  public void setInstanceProperty(String instanceName,
      InstancePropertyType type, String key, ZNRecord value)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type);
    path = path + "/" + key;
    
    try
    {
      _readWriteLock.writeLock().lock();
      _store.setProperty(path, value);
    }
    catch (PropertyStoreException e)
    {
      logger.error("Fail to set instance property, instance:" + instanceName +
                " type:" + type + " key:" + key + "\nexception:" + e);
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }
    
  }

  @Override
  public ZNRecord getInstanceProperty(String instanceName,
      InstancePropertyType type, String key)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type);
    path = path + "/" + key;
    try
    {
      _readWriteLock.readLock().lock();
      return _store.getProperty(path);
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to get instance property cluster:" + _clusterName + 
          " instanceName:" + instanceName + "type:" + type +
          " key:" + key + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
   
    return null;
  }

  @Override
  public List<ZNRecord> getInstancePropertyList(String instanceName,
      InstancePropertyType type)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type);

    return getChildRecords(path);
  }

  @Override
  public void removeInstanceProperty(String instanceName, InstancePropertyType type, 
      String key)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type);
    path = path + "/" + key;
    
    try
    {
      _readWriteLock.writeLock().lock();
      _store.removeProperty(path);
    }
    catch (PropertyStoreException e)
    {
      logger.error("Fail to remove instance property, instance:" + instanceName + 
          " type:" + type + " key:" + key  + "\nexception:" + e);
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }

  }
  
  @Override
  public void updateInstanceProperty(String instanceName,
      InstancePropertyType type, String key, ZNRecord value)
  {
    try
    {
      _readWriteLock.writeLock().lock();
      String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type) + "/" + key;
      if (type.isUpdateOnlyOnExists())
      {
        updateIfExists(path, value, type.isMergeOnUpdate());
      } else
      {
        createOrUpdate(path, value, type.isMergeOnUpdate());
      }
    }
    catch (PropertyStoreException e)
    {
      logger.error("fail to update instance property, instance:" + instanceName + 
          " type:" + type + " key:" + key + "\nexception:" + e);
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }

  }

  @Override
  public void removeClusterProperty(PropertyType type, String key)
  {
    // LOG.error("removeClusterProperty() NOT supported, type:" + type +
    //    " key:" + key);
    throw new UnsupportedOperationException(
      "removeClusterProperty() is NOT supported by FileDataAccessor");

  }

  @Override
  public void setClusterProperty(PropertyType type,
      String key, ZNRecord value, CreateMode mode)
  {
    setClusterProperty(type, key, value);
  }

  @Override
  public void setInstanceProperty(String instanceName,
      InstancePropertyType type, String subPath, String key,
      ZNRecord value)
  {
    if (subPath.indexOf('/')  > 0)
    {
      // LOG.error("setInstanceProperty() with multiple subPath NOT supported, subPath:" 
      //  + subPath);
      throw new UnsupportedOperationException(
        "setInstanceProperty() with multiple subPath is NOT supported by FileDataAccessor");

    }
    
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type);
    path = path + "/" + subPath;
    try
    {
      _readWriteLock.writeLock().lock();
      if (!_store.exists(path))
      {
        _store.createPropertyNamespace(path);
      }
      
      path = path + "/" + key; 
      _store.setProperty(path, value);
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to set instance property cluster:" + _clusterName + 
          " type:" + type + " subPath:" + subPath +
          " key:" + key + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }
  }

  
  @Override
  public void updateInstanceProperty(String instanceName, InstancePropertyType type, 
      String subPath, String key, ZNRecord value)
  {
    
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type);
    String parentPath = path + "/" + subPath;
    try
    {
      _readWriteLock.writeLock().lock();
    
      if (!_store.exists(parentPath))
      {
        String[] subPaths = subPath.split("/");
        String tempPath = path;
        for (int i = 0; i < subPaths.length; i++)
        {
          tempPath = tempPath + "/" + subPaths[i];
          if (!_store.exists(tempPath))
          {
            _store.createPropertyNamespace(tempPath);
          }
        }
      }
        
      String propertyPath = parentPath + "/" + key;
      createOrUpdate(propertyPath, value, type.isMergeOnUpdate());
    }
    catch (PropertyStoreException e)
    {
      logger.error("fail to update instance property, instance:" + instanceName + 
          " type:" + type +
          " subPath:" + subPath + " key:" + key + "\nexception:" + e);    
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }

  }

  @Override
  public List<ZNRecord> getInstancePropertyList(String instanceName,
      String subPath, InstancePropertyType type)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type);
    path = path + "/" + subPath;

    return getChildRecords(path);
  }

  @Override
  public ZNRecord getInstanceProperty(String instanceName,
      InstancePropertyType type, String subPath, String key)
  {
    if (subPath.indexOf('/')  > 0)
    {
      // LOG.error("getInstanceProperty() with multiple subPath NOT supported, subPath:" 
      //  + subPath);
      throw new UnsupportedOperationException(
        "getInstanceProperty() with multiple subPath is NOT supported by FileDataAccessor");

    }
    
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type);
    path = path + "/" + subPath + "/" + key;
    try
    {
      _readWriteLock.readLock().lock();
      if (_store.exists(path))
      {
        return _store.getProperty(path);
      }
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to get instance property cluster:" + _clusterName + 
          " type:" + type + " subPath:" + subPath +
          " key:" + key  + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
   
    return null;
  }

  @Override
  public List<String> getInstancePropertySubPaths(String instanceName,
      InstancePropertyType type)
  {
    // throw new UnsupportedOperationException(
    //  "getInstancePropertySubPaths() is NOT supported by FileDataAccessor");
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type);
    try
    {
      _readWriteLock.readLock().lock();
      return _store.getPropertyNames(path);
    }
    catch (PropertyStoreException e)
    {
      logger.error("fail to get instance property subPaths, instance:" + instanceName +
                   ", type:" + type + "\nexception:" + e);
      return null;
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
  }

  @Override
  public void substractInstanceProperty(String instanceName,
      InstancePropertyType type, String subPath, String key,
      ZNRecord value)
  {
    try
    {
      _readWriteLock.writeLock().lock();
      
      String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type) + 
          "/" + subPath + "/" + key;
      if (_store.exists(path))
      {
        ZNRecord curRecord = _store.getProperty(path);
        curRecord.substract(value);
        _store.setProperty(path, curRecord);
      }
      else
      {
        logger.warn("instance property to substract does NOT exist, instance:" + instanceName + 
                  " type:" + type + " subPath:" + subPath + " key:" + key);
      }
    }
    catch (PropertyStoreException e)
    {
      logger.error("fail to substract instance property, instance:" + instanceName + 
          " type:" + type + " subPath:" + subPath + " key:" + key + "\nexception:" + e);
      
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }

  }

  @Override
  public void createControllerProperty(ControllerPropertyType controllerProperty, 
      ZNRecord value, CreateMode mode)
  {
    // LOG.error("createControllerProperty() NOT supported");
    throw new UnsupportedOperationException(
      "createControllerProperty() is NOT supported by FileDataAccessor");

  }

  @Override
  public void removeControllerProperty(ControllerPropertyType type)
  {
    // LOG.error("removeControllerProperty() NOT supported, type:" + type);
    throw new UnsupportedOperationException(
      "removeControllerProperty() is NOT supported by FileDataAccessor");

  }

  @Override
  public void setControllerProperty(ControllerPropertyType type, 
      ZNRecord value, CreateMode mode)
  {
    // LOG.error("setControllerProperty() NOT supported, type:" + type);
    throw new UnsupportedOperationException(
      "setControllerProperty() is NOT supported by FileDataAccessor");

  }

  @Override
  public ZNRecord getControllerProperty(ControllerPropertyType type)
  {
    // LOG.error("getControllerProperty() NOT supported, type:" + type);
    // return null;
    throw new UnsupportedOperationException(
      "getControllerProperty() is NOT supported by FileDataAccessor");

  }
  
  private List<ZNRecord> getChildRecords(String parentPath)
  {
    List<ZNRecord> childRecords = new ArrayList<ZNRecord>();
    try
    {
      _readWriteLock.readLock().lock();
      
      List<String> childs = _store.getPropertyNames(parentPath);
      if (childs == null)
      {
        return childRecords;
      }
      
      for (String child : childs)
      {
        ZNRecord record = _store.getProperty(child);
        if (record != null)
        {
          childRecords.add(record);
        }
      }
      return childRecords;
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to get child properties cluster:" + _clusterName + 
          " parentPath:" + parentPath + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
    
    return childRecords;
  }
  
  @Override
  public PropertyStore<ZNRecord> getStore()
  {
    return _store;
  }

  @Override
  public void removeControllerProperty(ControllerPropertyType messages,
      String id)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("removeControllerProperty() is not implemented for file-based cm");
  }

  @Override
  public void setControllerProperty(ControllerPropertyType controllerProperty,
      String subPath, ZNRecord value, CreateMode mode)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setControllerProperty() is not implemented for file-based cm");
  }

  @Override
  public ZNRecord getControllerProperty(ControllerPropertyType controllerProperty, String subPath)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getControllerProperty() is not implemented for file-based cm");
  }
  */
  
  @Override
  public boolean setProperty(PropertyType type, ZNRecord value, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    
    try
    {
      _readWriteLock.writeLock().lock();
      _store.setProperty(path, value);
      return true;
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to set cluster property clusterName: " + _clusterName + 
                " type:" + type +
                " keys:" + keys + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }
    return false;
  }

  @Override
  public boolean updateProperty(PropertyType type, ZNRecord value,
      String... keys)
  {
    try
    {
      _readWriteLock.writeLock().lock();
      String path = PropertyPathConfig.getPath(type, _clusterName, keys); 
     if (type.isUpdateOnlyOnExists())
      {
       updateIfExists(path, value, type.isMergeOnUpdate());
      } else
      {
        createOrUpdate(path, value, type.isMergeOnUpdate());
      }
      return true;
    }
    catch (PropertyStoreException e)
    {
      logger.error("fail to update instance property, " + 
          " type:" + type + " keys:" + keys + "\nexception:" + e);
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }
    return false;

  }

  @Override
  public ZNRecord getProperty(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);

    try
    {
      _readWriteLock.readLock().lock();
      return _store.getProperty(path);
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to get cluster property clusterName: " + _clusterName + 
                " type:" + type +
                " keys:" + keys + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
    return null;
  }

  @Override
  public boolean removeProperty(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    
    try
    {
      _readWriteLock.writeLock().lock();
      _store.removeProperty(path);
      return true;
    }
    catch (PropertyStoreException e)
    {
      logger.error("Fail to remove instance property, "  + 
          " type:" + type + " keys:" + keys  + "\nexception:" + e);
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }

    return false;
  }

  @Override
  public List<String> getChildNames(PropertyType type, String... keys)
  {
    // List<String> childNames = new ArrayList<String>();
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    
    try
    {
      _readWriteLock.readLock().lock();
      
      List<String> childs = _store.getPropertyNames(path);
      return childs;
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to get child names:" + _clusterName + 
          " parentPath:" + path + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
    
    return Collections.emptyList();
  }

  @Override
  public List<ZNRecord> getChildValues(PropertyType type, String... keys)
  {
    List<ZNRecord> childRecords = new ArrayList<ZNRecord>();
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    
    try
    {
      _readWriteLock.readLock().lock();
      
      List<String> childs = _store.getPropertyNames(path);
      if (childs == null)
      {
        return childRecords;
      }
      
      for (String child : childs)
      {
        ZNRecord record = _store.getProperty(child);
        if (record != null)
        {
          childRecords.add(record);
        }
      }
      return childRecords;
    }
    catch(PropertyStoreException e)
    {
      logger.error("Fail to get child properties cluster:" + _clusterName + 
          " parentPath:" + path + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
    
    return childRecords;
  }

  @Override
  public PropertyStore<ZNRecord> getStore()
  {
    return _store;
  }
  
  private void updateIfExists(String path, final ZNRecord record, boolean mergeOnUpdate) 
  throws PropertyStoreException
  {
    if (_store.exists(path))
    {
      _store.setProperty(path, record);
    }
  }
  
  private void createOrUpdate(String path, ZNRecord record, boolean mergeOnUpdate) 
  throws PropertyStoreException
  {
    if (_store.exists(path))
    {
      ZNRecord curRecord = _store.getProperty(path);
      if (mergeOnUpdate)
      {
        curRecord.merge(record);
        _store.setProperty(path, curRecord);
      }
      else
      {
        _store.setProperty(path, record);
      }
    }
    else
    {
      _store.setProperty(path, record);
    }
  }

}
