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
package com.linkedin.helix.manager.file;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.PropertyStoreException;
import com.linkedin.helix.store.file.FilePropertyStore;

@Deprecated
public class FileDataAccessor implements DataAccessor
{
  private static Logger LOG = Logger.getLogger(FileDataAccessor.class);
  // store that is used by FileDataAccessor
  private final FilePropertyStore<ZNRecord> _store;
  private final String _clusterName;
  private final ReadWriteLock _readWriteLock = new ReentrantReadWriteLock();

  public FileDataAccessor(FilePropertyStore<ZNRecord> store, String clusterName)
  {
    _store = store;
    _clusterName = clusterName;
  }

  @Override
  public boolean setProperty(PropertyType type, HelixProperty value, String... keys)
  {
    return setProperty(type, value.getRecord(), keys);
  }

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
      LOG.error("Fail to set cluster property clusterName: " + _clusterName +
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
  public boolean updateProperty(PropertyType type, HelixProperty value, String... keys)
  {
    return updateProperty(type, value.getRecord(), keys);
  }

  @Override
  public boolean updateProperty(PropertyType type, ZNRecord value, String... keys)
  {
    try
    {
      _readWriteLock.writeLock().lock();
      String path = PropertyPathConfig.getPath(type, _clusterName, keys);
      if (type.isUpdateOnlyOnExists())
      {
        updateIfExists(path, value, type.isMergeOnUpdate());
      }
      else
      {
        createOrUpdate(path, value, type.isMergeOnUpdate());
      }
      return true;
    }
    catch (PropertyStoreException e)
    {
      LOG.error("fail to update instance property, " +
          " type:" + type + " keys:" + keys, e);
    }
    finally
    {
      _readWriteLock.writeLock().unlock();
    }
    return false;

  }

  @Override
  public <T extends HelixProperty>
    T getProperty(Class<T> clazz, PropertyType type, String... keys)
  {
    ZNRecord record = getProperty(type, keys);
    if (record == null)
    {
      return null;
    }
    return HelixProperty.convertToTypedInstance(clazz, record);
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
      LOG.error("Fail to get cluster property clusterName: " + _clusterName +
                " type:" + type +
                " keys:" + keys, e);
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
      LOG.error("Fail to remove instance property, "  +
          " type:" + type + " keys:" + keys, e);
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
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);

    try
    {
      _readWriteLock.readLock().lock();

      List<String> childs = _store.getPropertyNames(path);
      return childs;
    }
    catch(PropertyStoreException e)
    {
      LOG.error("Fail to get child names:" + _clusterName +
          " parentPath:" + path + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }

    return Collections.emptyList();
  }

  @Override
  public <T extends HelixProperty>
    List<T> getChildValues(Class<T> clazz, PropertyType type, String... keys)
  {
    List<ZNRecord> list = getChildValues(type, keys);
    return HelixProperty.convertToTypedList(clazz, list);
  }

  @Override
  public List<ZNRecord> getChildValues(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    List<ZNRecord> records = new ArrayList<ZNRecord>();
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
          records.add(record);
        }
      }
    }
    catch(PropertyStoreException e)
    {
      LOG.error("Fail to get child properties cluster:" + _clusterName +
          " parentPath:" + path + "\nexception: " + e);
    }
    finally
    {
      _readWriteLock.readLock().unlock();
    }
    return records;
  }

  // HACK remove it later
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

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(Class<T> clazz,
      PropertyType type, String... keys)
  {
    List<T> list = getChildValues(clazz, type, keys);
    return HelixProperty.convertListToMap(list);
  }
}
