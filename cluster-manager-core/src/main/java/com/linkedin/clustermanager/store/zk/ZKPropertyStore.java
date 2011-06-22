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

  private Map<PropertyChangeListener<T>, ZKPropertyStoreListeners> _listenerMap;

  private Map<String, T> _propertyValueCacheMap;

  private class ZKPropertyStoreListeners
  {
    public IZkDataListener _zkDataListener;
    public IZkChildListener _zkChildListener;
  }

  public ZKPropertyStore(String zkServers, final PropertySerializer serializer, String rootPath)
      throws PropertyStoreException
  {

    // String zkServers = "localhost:2188";

    _serializer = serializer;

    ZKClientFactory zkClientFactory = new ZKClientFactory();
    _zkClient = zkClientFactory.create(zkServers);
    setPropertySerializer(serializer);

    _rootPath = rootPath;

    _listenerMap = new ConcurrentHashMap<PropertyChangeListener<T>, ZKPropertyStoreListeners>();
    _propertyValueCacheMap = new TreeMap<String, T>();

  }

  private String stripOffTrailingSlash(String path)
  {
    // Strip off trailing slash
    while (path.endsWith("/"))
    {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  @Override
  public void setProperty(String key, final T value) throws PropertyStoreException
  {
    String path = stripOffTrailingSlash(key);

    _propertyValueCacheMap.put(path, value);

    // create recursively all the non-exist nodes
    String parent = path;
    LinkedList<String> pathsToCreate = new LinkedList();
    while (!_zkClient.exists(parent))
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

  @Override
  public T getProperty(String key) throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = stripOffTrailingSlash(key);

    T value = _propertyValueCacheMap.get(path);
    if (value != null)
    {
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
    String path = stripOffTrailingSlash(key);

    _propertyValueCacheMap.remove(path);

    if (!_zkClient.exists(path))
      return;

    // _zkClient.deleteRecursive(path);
    _zkClient.delete(path);
  }

  @Override
  public List<String> getPropertyNames(String prefix) throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = stripOffTrailingSlash(prefix);

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

  @Override
  public void subscribeForPropertyChange(String prefix, final PropertyChangeListener<T> listener)
      throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = stripOffTrailingSlash(prefix);
    if (!_zkClient.exists(path))
      return;

    // subscribe to data/children changes on node pointed by path
    IZkDataListener dataListener = new IZkDataListener() {

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
        // System.out.println(dataPath + ": deleted");
      }

    };
    _zkClient.subscribeDataChanges(path, dataListener);

    IZkChildListener childListener = new IZkChildListener() {

      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
      {
        // TODO Auto-generated method stub
        System.out.println(parentPath + ": child changes");
        System.out.println(currentChilds);
      }

    };
    _zkClient.subscribeChildChanges(path, childListener);

    // put mappings
    ZKPropertyStoreListeners listeners = new ZKPropertyStoreListeners();
    listeners._zkDataListener = dataListener;
    listeners._zkChildListener = childListener;

    _listenerMap.put(listener, listeners);

  }

  @Override
  public void unsubscribeForPropertyChange(String prefix, PropertyChangeListener<T> listener)
      throws PropertyStoreException
  {
    // TODO Auto-generated method stub
    String path = stripOffTrailingSlash(prefix);
    if (!_zkClient.exists(path))
      return;

    ZKPropertyStoreListeners listeners = _listenerMap.get(listener);
    if (listeners == null)
      return;

    _zkClient.unsubscribeDataChanges(path, listeners._zkDataListener);
    _zkClient.unsubscribeChildChanges(path, listeners._zkChildListener);
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
        }

        return obj;
      }

    };

    // _zkClient.setZkSerializer
    _zkClient.setZkSerializer(zkSerializer);
  }

  // for test purpose
  private static class StringSerializer implements PropertySerializer
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

  public static void main(String[] args) throws Exception
  {

    // use zk-client lib
    /*
     * String zkServers = "localhost:2188"; int sessionTimeout = 4000; int
     * connectTimeout = 10000; ZkSerializer zkSerializer = new ZkSerializer() {
     * 
     * @Override public byte[] serialize(Object data) throws ZkMarshallingError
     * { // TODO Auto-generated method stub String str = (String) data; byte[]
     * ret = str == null ? new byte[0] : str.getBytes(); return ret; }
     * 
     * @Override public Object deserialize(byte[] bytes) throws
     * ZkMarshallingError { // TODO Auto-generated method stub String ret = new
     * String(bytes); return ret; }
     * 
     * }; ZkClient client = new ZkClient(zkServers, sessionTimeout,
     * connectTimeout, zkSerializer);
     * 
     * // add/del a leaf String testRoot = "/testPath1"; //
     * client.delete("/testPath1/testPath3"); //
     * client.createPersistent("/testPath1/testPath3", "testData3");
     * client.deleteRecursive(testRoot); client.createPersistent(testRoot);
     * 
     * // subscribe/unsubscribe client.subscribeDataChanges(testRoot, new
     * IZkDataListener() {
     * 
     * @Override public void handleDataChange(String dataPath, Object data)
     * throws Exception { // TODO Auto-generated method stub
     * System.out.println(dataPath + ": changed to " + data); }
     * 
     * @Override public void handleDataDeleted(String dataPath) throws Exception
     * { // TODO Auto-generated method stub System.out.println(dataPath +
     * ": deleted"); }
     * 
     * });
     * 
     * client.subscribeChildChanges(testRoot, new IZkChildListener() {
     * 
     * @Override public void handleChildChange(String parentPath, List<String>
     * currentChilds) throws Exception { // TODO Auto-generated method stub
     * System.out.println(parentPath + ": children changed");
     * System.out.println(currentChilds); }
     * 
     * });
     * 
     * client.subscribeStateChanges(new IZkStateListener() {
     * 
     * @Override public void handleStateChanged(KeeperState state) throws
     * Exception { // TODO Auto-generated method stub
     * System.out.println("state changed to: " + state); }
     * 
     * @Override public void handleNewSession() throws Exception { // TODO
     * Auto-generated method stub System.out.println("new session created"); }
     * 
     * });
     * 
     * client.writeData(testRoot, "testData1"); String data =
     * client.readData(testRoot); System.out.println("read from " + testRoot +
     * ": " + data);
     * 
     * String testChild1 = testRoot + "/testPath2";
     * client.createPersistent(testChild1, "testData2");
     */

    String zkServers = "localhost:2188";
    ZKPropertyStore<String> zkPropertyStore = new ZKPropertyStore<String>(zkServers,
        new StringSerializer(), "/schemas");

    String testRoot = "/testPath1";

    String value = zkPropertyStore.getProperty(testRoot);
    System.out.println(value);

    // zkPropertyStore.getZkClient().writeData(testRoot, "testData1-v0");
    // zkPropertyStore.setProperty(testRoot, "testData1-v0");

    // String temp = "/abc/def";
    // System.out.println(temp.substring(0, temp.lastIndexOf('/')));
    Thread.currentThread().sleep(5000);
  }
}
