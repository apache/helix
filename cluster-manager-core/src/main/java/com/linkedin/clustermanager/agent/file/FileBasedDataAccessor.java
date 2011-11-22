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
