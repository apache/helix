package com.linkedin.clustermanager.store.zk;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertySerializer;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.store.PropertyStoreException;

public class ZKPropertyStore<T> implements PropertyStore<T>
{
  private final String _ROOT = "";
  protected final ZkClient _zkClient;
  protected final PropertySerializer _serializer;
  protected final String _rootPath;

  private Map<String, Map<PropertyChangeListener<T>, ZKPropertyListenerTuple>> _listenerMap = new ConcurrentHashMap<String, Map<PropertyChangeListener<T>, ZKPropertyListenerTuple>>();
  private Map<String, T> _propertyValueCacheMap = new ConcurrentHashMap<String, T>();

  // statistics numbers
  private long _nrReads = 0;
  private long _nrHits = 0;

  // 1-1 mapping from a PropertyChangeListener<T> to IZKxxx listeners
  private class ZKPropertyListenerTuple
  {
    public final IZkDataListener _zkDataListener;
    public final IZkChildListener _zkChildListener;

    public ZKPropertyListenerTuple(final PropertyChangeListener<T> listener)
    {
      _zkDataListener = new IZkDataListener() {

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception
        {
          // TODO Auto-generated method stub
          System.out.println(dataPath + ": data changed to " + data);
          listener.onPropertyChange(getRelativePath(dataPath));
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception
        {
          // TODO Auto-generated method stub
          System.out.println("property deleted at " + dataPath);

          // need to delete data listener put by its parent and property change
          // listener by itself
          _zkClient.unsubscribeDataChanges(dataPath, this);

          unsubscribeForPropertyChange(getRelativePath(dataPath), listener);
        }

      };

      _zkChildListener = new IZkChildListener() {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds)
            throws Exception
        {
          // TODO Auto-generated method stub
          System.out.println("children changed at " + parentPath + ": " + currentChilds);

          for (String child : currentChilds)
          {
            String pathToChild = parentPath + "/" + child;
            _zkClient.subscribeDataChanges(pathToChild, _zkDataListener);
            listener.onPropertyChange(getRelativePath(pathToChild));
          }
        }

      };

    }
  }

  public ZKPropertyStore(ZkClient zkClient, final PropertySerializer serializer, String rootPath)
  {
    _serializer = serializer;

    _zkClient = zkClient;
    setPropertySerializer(serializer);

    // Strip off trailing slash
    while (rootPath.endsWith("/"))
    {
      rootPath = rootPath.substring(0, rootPath.length() - 1);
    }

    _rootPath = rootPath;
  }

  public double getHitRatio()
  {
    System.out.println("Reads(" + _nrReads + ")/Hits(" + _nrHits + ")");
    if (_nrReads == 0)
      return 0.0;
    return (double) _nrHits / _nrReads;
  }

  private String getPath(String key)
  {
    // Strip off trailing slash
    while (key.endsWith("/"))
    {
      key = key.substring(0, key.length() - 1);
    }

    String path;
    path = _rootPath + "/" + key;

    // Strip off trailing slash
    if (path.endsWith("/"))
    {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  private String getRelativePath(String path)
  {
    // strip off rootPath from path
    if (!path.startsWith(_rootPath))
    {
      System.err.println("path does NOT start with: " + _rootPath);
      return path;
    }

    path = path.substring(_rootPath.length());

    if (path.startsWith("/"))
      path = path.substring(1);

    return path;
  }

  @Override
  public synchronized void setProperty(String key, final T value) throws PropertyStoreException
  {
    String path = getPath(key);

    // create recursively all the non-exist nodes
    String parent = path;
    LinkedList<String> pathsToCreate = new LinkedList<String>();
    while (parent.length() > 0 && !_zkClient.exists(parent))
    {
      pathsToCreate.push(parent);
      parent = parent.substring(0, parent.lastIndexOf('/'));
    }

    while (pathsToCreate.size() > 0)
    {
      String node = pathsToCreate.pop();
      _zkClient.createPersistent(node);
    }

    _propertyValueCacheMap.put(path, value);
    _zkClient.writeData(path, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized T getProperty(String key) throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = getPath(key);
    _nrReads++;

    T value = null;

    value = _propertyValueCacheMap.get(path);

    if (value != null)
    {
      _nrHits++;
    } else
    {
      try
      {
        value = (T) _zkClient.readData(path);
        _propertyValueCacheMap.put(path, value);
      } catch (Exception e) // ZkNoNodeException
      {
        System.err.println(e.getMessage());
        // throw (new PropertyStoreException(e.getMessage()));
      }

    }

    return value;
  }

  @Override
  public synchronized void removeProperty(String key) throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = getPath(key);

    try
    {
      _zkClient.delete(path);

      // even if path is valid, we may not be able to remove
      // since it has children
      // in this case, can't remove it from maps
      _propertyValueCacheMap.remove(path);
      _listenerMap.remove(path);
    } catch (Exception e)
    {
      System.err.println(e.getMessage());
      throw (new PropertyStoreException(e.getMessage()));
    }

  }

  public String getRootPropertyName()
  {
    return _rootPath;
  }

  public void removeRootProperty() throws PropertyStoreException
  {
    removeProperyRecursive(_ROOT);
  }

  public synchronized void removeProperyRecursive(String key) throws PropertyStoreException
  {
    String path = getPath(key);
    doRemovePropertyRecursive(path);
  }

  private boolean doRemovePropertyRecursive(String path) throws PropertyStoreException
  {
    List<String> children;
    try
    {
      children = _zkClient.getChildren(path);
    } catch (ZkNoNodeException e)
    {
      return true;
    }

    for (String subPath : children)
    {
      if (!doRemovePropertyRecursive(path + "/" + subPath))
      {
        return false;
      }
    }

    _listenerMap.remove(path);
    _propertyValueCacheMap.remove(path);

    return _zkClient.delete(path);
  }

  @Override
  public List<String> getPropertyNames(String prefix) throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = getPath(prefix);

    if (!_zkClient.exists(path))
      return null;

    List<String> children = _zkClient.getChildren(path);

    List<String> propertyNames = new ArrayList<String>();
    for (String child : children)
    {
      String propertyName = path + "/" + child;
      propertyNames.add(getRelativePath(propertyName));
    }
    return propertyNames;
  }

  @Override
  public void setPropertyDelimiter(String delimiter) throws PropertyStoreException
  {
    // TODO Auto-generated method stub

  }

  public void subscribeForRootPropertyChange(final PropertyChangeListener<T> listener)
      throws PropertyStoreException
  {
    subscribeForPropertyChange(_ROOT, listener);
  }

  // put listener on prefix and all its children
  @Override
  public void subscribeForPropertyChange(String prefix, final PropertyChangeListener<T> listener)
      throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = getPath(prefix);
    if (!_zkClient.exists(path))
      return;

    Map<PropertyChangeListener<T>, ZKPropertyListenerTuple> listenerMapForPath = null;
    synchronized (_listenerMap)
    {
      listenerMapForPath = _listenerMap.get(path);
      if (listenerMapForPath == null)
      {
        listenerMapForPath = new ConcurrentHashMap<PropertyChangeListener<T>, ZKPropertyListenerTuple>();
        _listenerMap.put(path, listenerMapForPath);
      }

      if (listenerMapForPath.get(listener) == null)
      {
        ZKPropertyListenerTuple listenerTuple = new ZKPropertyListenerTuple(listener);
        listenerMapForPath.put(listener, listenerTuple);

        // add data listener on prefix and all its children
        List<String> children = _zkClient.getChildren(path);

        // _zkClient.subscribeDataChanges(path, listenerTuple._zkDataListener);
        for (String child : children)
        {
          String pathToChild = path + "/" + child;
          _zkClient.subscribeDataChanges(pathToChild, listenerTuple._zkDataListener);
        }

        // add child listener on prefix only
        _zkClient.subscribeChildChanges(path, listenerTuple._zkChildListener);
      }

    }
  }

  public void unsubscribeForRootPropertyChange(PropertyChangeListener<T> listener)
      throws PropertyStoreException
  {
    unsubscribeForPropertyChange(_ROOT, listener);
  }

  @Override
  public void unsubscribeForPropertyChange(String prefix, PropertyChangeListener<T> listener)
      throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = getPath(prefix);
    // if (!_zkClient.exists(path))
    // return;

    synchronized (_listenerMap)
    {
      final Map<PropertyChangeListener<T>, ZKPropertyListenerTuple> listenerMapForPath = _listenerMap
          .get(path);
      if (listenerMapForPath != null)
      {
        ZKPropertyListenerTuple listenerTuple = listenerMapForPath.remove(listener);

        if (listenerTuple != null)
        {
          // _zkClient.unsubscribeDataChanges(path,
          // listenerTuple._zkDataListener);
          List<String> children = _zkClient.getChildren(path);
          for (String child : children)
          {
            String pathToChild = path + "/" + child;
            _zkClient.unsubscribeDataChanges(pathToChild, listenerTuple._zkDataListener);
          }

          _zkClient.unsubscribeChildChanges(path, listenerTuple._zkChildListener);
        }
      }

      if (listenerMapForPath == null || listenerMapForPath.isEmpty())
      {
        _listenerMap.remove(path);
      }

    }
  }

  @Override
  public boolean canParentStoreData()
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setPropertySerializer(final PropertySerializer serializer)
  {
    // TODO Auto-generated method stub
    ZkSerializer zkSerializer = new ZkSerializer() {

      @Override
      public byte[] serialize(Object data) throws ZkMarshallingError
      {
        // TODO Auto-generated method stub
        byte[] bytes = null;
        try
        {
          bytes = serializer.serialize(data);
        } catch (PropertyStoreException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
          throw new ZkMarshallingError(e.getMessage());
        }
        return bytes;
      }

      @Override
      public Object deserialize(byte[] bytes) throws ZkMarshallingError
      {
        // TODO Auto-generated method stub
        Object obj = null;
        try
        {
          obj = serializer.deserialize(bytes);
        } catch (PropertyStoreException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
          throw new ZkMarshallingError(e.getMessage());
        }

        return obj;
      }

    };

    _zkClient.setZkSerializer(zkSerializer);
  }

  // for test purpose
  private static class MyStringSerializer implements PropertySerializer
  {

    @Override
    public byte[] serialize(Object data) throws PropertyStoreException
    {
      String value = (String) data;

      return value.getBytes();
    }

    @Override
    public Object deserialize(byte[] bytes) throws PropertyStoreException
    {

      return new String(bytes);
    }

  }

  private static class MyPropertyChangeListener implements PropertyChangeListener<String>
  {

    @Override
    public void onPropertyChange(String key)
    {
      // TODO Auto-generated method stub
      System.out.println("property changed at " + key);
    }

  }

  public static void main(String[] args) throws Exception

  {/*
    * // start zk severs List<Integer> localPorts = new ArrayList<Integer>();
    * localPorts.add(2188);
    * 
    * List<ZkServer> localZkServers =
    * TestZKCallback.startLocalZookeeper(localPorts,
    * System.getProperty("user.dir") + "/" + "zkdata", 2000);
    * 
    * System.out.println("zk servers started on ports: " + localPorts);
    * 
    * String zkServers = "localhost:2188"; ZKClientFactory zkClientFactory = new
    * ZKClientFactory(); ZkClient zkClient = zkClientFactory.create(zkServers);
    * 
    * String propertyStoreRoot = "/testPath1"; ZKPropertyStore<String>
    * zkPropertyStore = new ZKPropertyStore<String>(zkClient, new
    * MyStringSerializer(), propertyStoreRoot);
    * 
    * zkPropertyStore.removeRootProperty();
    * 
    * String testPath2 = "testPath2"; zkPropertyStore.setProperty(testPath2,
    * "testData2_I");
    * 
    * String value = zkPropertyStore.getProperty(testPath2);
    * System.out.println("Read from " + testPath2 + ": " + value);
    * 
    * MyPropertyChangeListener listener = new MyPropertyChangeListener();
    * zkPropertyStore.subscribeForRootPropertyChange(listener); // duplicated
    * listener should have no effect //
    * zkPropertyStore.subscribeForRootPropertyChange(listener);
    * 
    * String testPath3 = "testPath3"; zkPropertyStore.setProperty(testPath3,
    * "testData3_I");
    * 
    * Thread.sleep(100);
    * 
    * zkPropertyStore.getProperty("abc");
    * 
    * try { zkPropertyStore.removeProperty(""); } catch (PropertyStoreException
    * e) { // e.printStackTrace(); System.err.println(e.getMessage()); }
    * 
    * zkPropertyStore.removeProperty("xyz");
    * 
    * zkPropertyStore.removeProperty(testPath2); Thread.sleep(100);
    * 
    * zkPropertyStore.setProperty(testPath3, "testData3_II"); Thread.sleep(100);
    * 
    * zkPropertyStore.unsubscribeForRootPropertyChange(listener);
    * zkPropertyStore.setProperty(testPath3, "testData3_III");
    * Thread.sleep(100);
    * 
    * value = zkPropertyStore.getProperty(testPath3);
    * System.out.println("Read from " + testPath3 + ": " + value);
    * 
    * System.out.println("Hit ratio = " + zkPropertyStore.getHitRatio());
    * 
    * // shutdown zk servers TestZKCallback.stopLocalZookeeper(localZkServers);
    * System.out.println("zk servers stopped");
    */
  }
}