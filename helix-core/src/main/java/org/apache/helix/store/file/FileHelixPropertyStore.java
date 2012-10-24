package org.apache.helix.store.file;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.PropertyChangeListener;
import org.apache.helix.store.PropertyJsonComparator;
import org.apache.helix.store.PropertySerializer;
import org.apache.helix.store.PropertyStat;
import org.apache.helix.store.PropertyStoreException;
import org.apache.zookeeper.data.Stat;


public class FileHelixPropertyStore<T> implements HelixPropertyStore<T>
{
  final FilePropertyStore<T> _store;

  public FileHelixPropertyStore(final PropertySerializer<T> serializer,
                                String rootNamespace,
                                final PropertyJsonComparator<T> comparator)
  {
    _store = new FilePropertyStore<T>(serializer, rootNamespace, comparator);
  }

  @Override
  public boolean create(String path, T record, int options)
  {
    return set(path, record, options);
  }

  @Override
  public boolean set(String path, T record, int options)
  {
    try
    {
      _store.setProperty(path, record);
      return true;
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public boolean update(String path, DataUpdater<T> updater, int options)
  {
    _store.updatePropertyUntilSucceed(path, updater);
    return true;
  }

  @Override
  public boolean remove(String path, int options)
  {
    try
    {
      _store.removeProperty(path);
      return true;
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public boolean[] createChildren(List<String> paths, List<T> records, int options)
  {
    return setChildren(paths, records, options);
  }

  @Override
  public boolean[] setChildren(List<String> paths, List<T> records, int options)
  {
    boolean[] success = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      success[i] = create(paths.get(i), records.get(i), options);
    }
    return success;
  }

  @Override
  public boolean[] updateChildren(List<String> paths,
                                  List<DataUpdater<T>> updaters,
                                  int options)
  {
    boolean[] success = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      success[i] = update(paths.get(i), updaters.get(i), options);
    }
    return success;

  }

  @Override
  public boolean[] remove(List<String> paths, int options)
  {
    boolean[] success = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      success[i] = remove(paths.get(i), options);
    }
    return success;
  }

  @Override
  public T get(String path, Stat stat, int options)
  {
    PropertyStat propertyStat = new PropertyStat();
    try
    {
      T value = _store.getProperty(path, propertyStat);
      if (stat != null)
      {
        stat.setVersion(propertyStat.getVersion());
        stat.setMtime(propertyStat.getLastModifiedTime());
      }
      return value;
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options)
  {
    List<T> values = new ArrayList<T>();
    for (int i = 0; i < paths.size(); i++)
    {
      values.add(get(paths.get(i), stats.get(i), options));
    }
    return values;
  }

  @Override
  public List<T> getChildren(String parentPath, List<Stat> stats, int options)
  {
    List<String> childNames = getChildNames(parentPath, options);
    
    if (childNames == null)
    {
      return null;
    }

    List<String> paths = new ArrayList<String>();
    for (String childName : childNames)
    {
      String path = parentPath + "/" + childName;
      paths.add(path);
    }
    
    return get(paths, stats, options);
  }

  @Override
  public List<String> getChildNames(String parentPath, int options)
  {
    try
    {
      return _store.getPropertyNames(parentPath);
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return Collections.emptyList();
  }

  @Override
  public boolean exists(String path, int options)
  {
    return _store.exists(path);
  }

  @Override
  public boolean[] exists(List<String> paths, int options)
  {
    boolean[] exists = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      exists[i] = exists(paths.get(i), options);
    }
    return exists;
  }

  @Override
  public Stat[] getStats(List<String> paths, int options)
  {
    Stat[] stats = new Stat[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      stats[i] = getStat(paths.get(i), options);
    }
    return stats;
  }

  @Override
  public Stat getStat(String path, int options)
  {
    Stat stat = new Stat();
    get(path, stat, options);
    return stat;
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener)
  {
    throw new UnsupportedOperationException("subscribeDataChanges not supported");
  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener)
  {
    throw new UnsupportedOperationException("unsubscribeDataChanges not supported");
  }

  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener)
  {
    throw new UnsupportedOperationException("subscribeChildChanges not supported");
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener)
  {
    throw new UnsupportedOperationException("unsubscribeChildChanges not supported");
  }

  @Override
  public void start()
  {
    _store.start();
  }

  @Override
  public void stop()
  {
    _store.stop();
  }

  @Override
  public void subscribe(String parentPath, final HelixPropertyListener listener)
  {
    try
    {
      _store.subscribeForPropertyChange(parentPath, new PropertyChangeListener<T>()
      {

        @Override
        public void onPropertyChange(String key)
        {
          listener.onDataChange(key);
        }
      });
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void unsubscribe(String parentPath, HelixPropertyListener listener)
  {
    throw new UnsupportedOperationException("unsubscribe not supported");
  }

  @Override
  public void reset()
  {
    // TODO Auto-generated method stub
    
  }

}
