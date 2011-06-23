package com.linkedin.clustermanager.store.zk;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
  protected final ZkClient _zkClient;
  protected final PropertySerializer _serializer;
  protected final String _rootPath;

  private Map<PropertyChangeListener<T>, ZKPropertyListenerPair> _listenerMap;
  private Map<String, T> _propertyValueCacheMap;

  // statistics numbers
  private long _nrReads = 0;
  private long _nrHits = 0;

  private class ZKPropertyListenerPair
  {
    public IZkDataListener _zkDataListener;
    public IZkChildListener _zkChildListener;
  }

  public ZKPropertyStore(ZkClient zkClient, final PropertySerializer serializer, String rootPath)
  {

    _serializer = serializer;

    _zkClient = zkClient;
    setPropertySerializer(serializer);

    _rootPath = rootPath;

    _listenerMap = new ConcurrentHashMap<PropertyChangeListener<T>, ZKPropertyListenerPair>();
    _propertyValueCacheMap = new TreeMap<String, T>();

  }

  public double getHitRatio()
  {
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
    if (key.equals(""))
      path = _rootPath;
    else
      path = _rootPath + "/" + key;

    return path;
  }

  @Override
  public void setProperty(String key, final T value) throws PropertyStoreException
  {
    String path = getPath(key);

    // _propertyValueCacheMap.put(path, value);

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

    _zkClient.writeData(path, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T getProperty(String key) throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = getPath(key);
    _nrReads++;

    T value = _propertyValueCacheMap.get(path);

    if (value != null)
    {
      _nrHits++;
      return value;
    }

    if (!_zkClient.exists(path))
      return null;

    value = (T) _zkClient.readData(path);
    _propertyValueCacheMap.put(path, value);

    return value;
  }

  @Override
  public void removeProperty(String key) throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = getPath(key);

    _propertyValueCacheMap.remove(path);

    if (!_zkClient.exists(path))
      return;

    _zkClient.delete(path);
  }

  public void removeProperyRecursive(String key) throws PropertyStoreException
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
      String propertyName = prefix + "/" + child;
      propertyNames.add(propertyName);
    }
    return propertyNames;
  }

  @Override
  public void setPropertyDelimiter(String delimiter) throws PropertyStoreException
  {
    // TODO Auto-generated method stub

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

    synchronized (_listenerMap)
    {
      ZKPropertyListenerPair listenerPair = _listenerMap.get(listener);
      if (listenerPair == null)
      {
        listenerPair = new ZKPropertyListenerPair();

        final IZkDataListener dataListener = new IZkDataListener() {

          @Override
          public void handleDataChange(String dataPath, Object data) throws Exception
          {
            // TODO Auto-generated method stub
            // System.out.println(dataPath + ": changed to " + data);
            listener.onPropertyChange(dataPath);
          }

          @Override
          public void handleDataDeleted(String dataPath) throws Exception
          {
            // TODO Auto-generated method stub
            System.out.println("property deleted at " + dataPath);
          }

        };

        final IZkChildListener childListener = new IZkChildListener() {

          @Override
          public void handleChildChange(String parentPath, List<String> currentChilds)
              throws Exception
          {
            // TODO Auto-generated method stub
            // System.out.println("children changed at " + parentPath + ": " +
            // currentChilds);

            for (String child : currentChilds)
            {
              String pathToChild = parentPath + "/" + child;
              _zkClient.subscribeDataChanges(pathToChild, dataListener);
            }
          }

        };

        listenerPair._zkDataListener = dataListener;
        listenerPair._zkChildListener = childListener;
        _listenerMap.put(listener, listenerPair);
      }

      // add data listener on prefix and all its children
      List<String> children = _zkClient.getChildren(path);

      _zkClient.subscribeDataChanges(path, listenerPair._zkDataListener);
      for (String child : children)
      {
        String pathToChild = path + "/" + child;
        _zkClient.subscribeDataChanges(pathToChild, listenerPair._zkDataListener);
      }

      // add child listener on prefix only
      _zkClient.subscribeChildChanges(path, listenerPair._zkChildListener);

    }
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
      ZKPropertyListenerPair listenerPair = _listenerMap.get(listener);
      if (listenerPair == null)
        return;

      _zkClient.unsubscribeDataChanges(path, listenerPair._zkDataListener);
      List<String> children = _zkClient.getChildren(path);
      for (String child : children)
      {
        String pathToChild = path + "/" + child;
        _zkClient.unsubscribeDataChanges(pathToChild, listenerPair._zkDataListener);
      }

      _zkClient.unsubscribeChildChanges(path, listenerPair._zkChildListener);
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

    // _zkClient.setZkSerializer
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
  {
    String zkServers = "localhost:2188";
    ZKClientFactory zkClientFactory = new ZKClientFactory();
    ZkClient zkClient = zkClientFactory.create(zkServers);

    String propertyStoreRoot = "/testPath1";
    ZKPropertyStore<String> zkPropertyStore = new ZKPropertyStore<String>(zkClient,
        new MyStringSerializer(), propertyStoreRoot);

    String root = "";
    zkPropertyStore.removeProperyRecursive(root);

    String testPath2 = "testPath2";
    zkPropertyStore.setProperty(testPath2, "testData2");

    String value = zkPropertyStore.getProperty(testPath2);
    System.out.println("Read from " + testPath2 + ": " + value);

    MyPropertyChangeListener listener = new MyPropertyChangeListener();
    zkPropertyStore.subscribeForPropertyChange(root, listener);
    // duplicated listener should have no effect
    zkPropertyStore.subscribeForPropertyChange(root, listener);

    String testPath3 = "testPath3";
    zkPropertyStore.setProperty(testPath3, "testData3");

    value = zkPropertyStore.getProperty(testPath3);
    System.out.println("Read from " + testPath3 + ": " + value);

    zkPropertyStore.removeProperty(testPath2);
    zkPropertyStore.setProperty(root, "testData1-2");
    zkPropertyStore.setProperty(testPath3, "testData3-2");

    zkPropertyStore.unsubscribeForPropertyChange(root, listener);
    zkPropertyStore.setProperty(testPath3, "testData3-3");
    zkPropertyStore.setProperty(root, "testData1-3");

    value = zkPropertyStore.getProperty(testPath3);
    System.out.println("Read from " + testPath3 + ": " + value);

    System.out.println("Hit ratio = " + zkPropertyStore.getHitRatio());

    Thread.currentThread().sleep(5000);
  }
}