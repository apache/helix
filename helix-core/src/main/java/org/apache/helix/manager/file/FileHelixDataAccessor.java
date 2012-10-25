package org.apache.helix.manager.file;


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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.store.PropertyStore;
import org.apache.helix.store.PropertyStoreException;
import org.apache.helix.store.file.FilePropertyStore;
import org.apache.log4j.Logger;


@Deprecated
public class FileHelixDataAccessor implements HelixDataAccessor
{
  private static Logger LOG = Logger.getLogger(FileHelixDataAccessor.class);

  private final FilePropertyStore<ZNRecord> _store;
  private final String _clusterName;
  private final ReadWriteLock _readWriteLock = new ReentrantReadWriteLock();
  private final Builder _propertyKeyBuilder;


  public FileHelixDataAccessor(FilePropertyStore<ZNRecord> store,
      String clusterName)
  {
    _store = store;
    _clusterName = clusterName;
    _propertyKeyBuilder = new PropertyKey.Builder(_clusterName);
  }

  @Override
  public boolean createProperty(PropertyKey key, HelixProperty value)
  {
    return updateProperty(key, value);
  }

  @Override
  public <T extends HelixProperty> boolean setProperty(PropertyKey key, T value)
  {
    String path = key.getPath();
    try
    {
      _readWriteLock.writeLock().lock();
      _store.setProperty(path, value.getRecord());
      return true;
    }
    catch(PropertyStoreException e)
    {
      LOG.error("Fail to set cluster property clusterName: " + _clusterName +
                " type:" + key.getType() +
                " keys:" + Arrays.toString(key.getParams()), e);
      return false;
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key,
      T value)
  {
    PropertyType type = key.getType();
    String path = key.getPath();

    try
    {
      _readWriteLock.writeLock().lock();
      
      if (type.isUpdateOnlyOnExists())
      {
        updateIfExists(path, value.getRecord(), type.isMergeOnUpdate());
      }
      else
      {
        createOrUpdate(path, value.getRecord(), type.isMergeOnUpdate());
      }
      return true;
    }
    catch (PropertyStoreException e)
    {
      LOG.error("fail to update property. type:" +
          type + ", keys:" + Arrays.toString(key.getParams()), e);
      return false;
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key)
  {
    String path = key.getPath();
    try
    {
      _readWriteLock.readLock().lock();
      ZNRecord record = _store.getProperty(path);
      if (record == null)
      {
        return null;
      }
      return (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
    }
    catch(PropertyStoreException e)
    {
      LOG.error("Fail to get property. clusterName: " + _clusterName +
                " type:" + key.getType() +
                " keys:" + Arrays.toString(key.getParams()), e);
      return null;
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
  }

  @Override
  public boolean removeProperty(PropertyKey key)
  {
    String path = key.getPath();;

    try
    {
      _readWriteLock.writeLock().lock();
      _store.removeProperty(path);
      return true;
    }
    catch (PropertyStoreException e)
    {
      LOG.error("Fail to remove property. type:"  +
          key.getType() + ", keys:" + Arrays.toString(key.getParams()), e);
      return false;
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public List<String> getChildNames(PropertyKey key)
  {
    String path = key.getPath();;

    try
    {
      _readWriteLock.readLock().lock();

      List<String> childs = _store.getPropertyNames(path);
      return childs;
    }
    catch(PropertyStoreException e)
    {
      LOG.error("Fail to get child names. clusterName: " + _clusterName +
          ", parentPath:" + path, e);
      
      return Collections.emptyList();
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key)
  {
    String path = key.getPath();
    List<T> records = new ArrayList<T>();
    try
    {
      _readWriteLock.readLock().lock();

      List<String> childs = _store.getPropertyNames(path);
      if (childs == null || childs.size() == 0)
      {
        return Collections.emptyList();
      }

      for (String child : childs)
      {
        ZNRecord record = _store.getProperty(child);
        if (record != null)
        {
          records.add((T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record));
        }
      }
    }
    catch(PropertyStoreException e)
    {
      LOG.error("Fail to get child properties. clusterName:" + _clusterName +
          ", parentPath:" + path, e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
    
    return records;
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(
      PropertyKey key)
  {
    List<T> list = getChildValues(key);
    return HelixProperty.convertListToMap(list);
  }

  @Override
  public Builder keyBuilder()
  {
    return _propertyKeyBuilder;
  }

  @Override
  public <T extends HelixProperty> boolean[] createChildren(
      List<PropertyKey> keys, List<T> children)
  {
    boolean[] success = new boolean[keys.size()];
    for (int i = 0; i < keys.size(); i++)
    {
      success[i] = createProperty(keys.get(i), children.get(i));
    }
    return success;
  }

  @Override
  public <T extends HelixProperty> boolean[] setChildren(
      List<PropertyKey> keys, List<T> children)
  {
    boolean[] success = new boolean[keys.size()];
    for (int i = 0; i < keys.size(); i++)
    {
      success[i] = setProperty(keys.get(i), children.get(i));
    }
    return success;
  }

  @Override
  public BaseDataAccessor getBaseDataAccessor()
  {
    throw new UnsupportedOperationException("No BaseDataAccessor for FileHelixDataAccessor");
  }
  
  // HACK remove it later
  public PropertyStore<ZNRecord> getStore()
  {
    return _store;
  }

  private void createOrUpdate(String path, final ZNRecord record, final boolean mergeOnUpdate)
      throws PropertyStoreException
  {
    final int RETRYLIMIT = 3;
    int retryCount = 0;
    while (retryCount < RETRYLIMIT)
    {
      try
      {
        if (_store.exists(path))
        {
          DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>()
          {
            @Override
            public ZNRecord update(ZNRecord currentData)
            {
              if(mergeOnUpdate)
              {
                currentData.merge(record);
                return currentData;
              }
              return record;
            }
          };
          _store.updatePropertyUntilSucceed(path, updater);

        }
        else
        {
          if(record.getDeltaList().size() > 0)
          {
            ZNRecord newRecord = new ZNRecord(record.getId());
            newRecord.merge(record);
            _store.setProperty(path, newRecord);
          }
          else
          {
            _store.setProperty(path, record);
          }
        }
        break;
      }
      catch (Exception e)
      {
        retryCount = retryCount + 1;
        LOG.warn("Exception trying to update " + path + " Exception:"
            + e.getMessage() + ". Will retry.");
      }
    }
  }
  
  private void updateIfExists(String path, final ZNRecord record, boolean mergeOnUpdate)
      throws PropertyStoreException
  {
    if (_store.exists(path))
    {
      _store.setProperty(path, record);
    }
  }

  @Override
  public <T extends HelixProperty> boolean[] updateChildren(List<String> paths,
      List<DataUpdater<ZNRecord>> updaters, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys)
  {
    List<T> list = new ArrayList<T>();
    for (PropertyKey key : keys)
    {
      list.add((T)getProperty(key));
    }
    return list;
  }
  
}
