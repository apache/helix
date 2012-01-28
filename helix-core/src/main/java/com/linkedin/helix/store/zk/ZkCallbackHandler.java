package com.linkedin.helix.store.zk;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.store.PropertyChangeListener;

public class ZkCallbackHandler<T> implements IZkChildListener, IZkDataListener
{
  private static Logger LOG = Logger.getLogger(ZkCallbackHandler.class);
  private final String ROOT = "/";

  private final ZkClient _zkClient;
  private final String _propertyStoreRoot;
  /*
   * prefixPath is the prefix path the PropertyChangeListener is listening on
   * any change under that prefix path should trigger the listener
   */
  private final String _prefixPath;
  private final PropertyChangeListener<T> _listener;

  public ZkCallbackHandler(ZkClient client, String root, String path,
                           PropertyChangeListener<T> listener)
  {
    _zkClient = client;
    _propertyStoreRoot = root;
    _prefixPath = path;
    _listener = listener;
    // init();
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception
  {
    if (LOG.isDebugEnabled())
    {
      LOG.debug(dataPath + ": data changed to " + data);
    }
    _listener.onPropertyChange(getRelativePath(dataPath));

  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception
  {
    if (LOG.isDebugEnabled())
    {
      LOG.debug(dataPath + ": data deleted");
    }
  }

  @Override
  public void handleChildChange(String path, List<String> currentChilds) throws Exception
  {
    LOG.debug("childs change, " + path + " : " + currentChilds);

    /*
     * When a node with a child change watcher is deleted 
     * a child change is trigger on the deleted node
     * and in this case, the currentChilds is null
     */
    if (currentChilds == null)
    {
      return;
    }
    else if (currentChilds.size() == 0)
    {
      // do initial invocation
      Object data = _zkClient.readData(path);
      this.handleDataChange(path, data);      
    }
    else    // currentChilds.size() > 0
    {
      for (String child : currentChilds)
      {
        String childPath = path.endsWith("/")? path + child : path + "/" + child;
        _zkClient.subscribeDataChanges(childPath, this);
        _zkClient.subscribeChildChanges(childPath, this);

        // recursive call
        this.handleChildChange(childPath, _zkClient.getChildren(childPath));
      }
    }
    
    // _listener.onPropertyChange(getRelativePath(path));
  }
  
  private String getRelativePath(String path)
  {
    // strip off rootPath from path
    if (!path.startsWith(_propertyStoreRoot))
    {
      LOG.warn("path does NOT start with: " + _propertyStoreRoot);
      return path;
    }

    if (path.equals(_propertyStoreRoot))
      return ROOT;

    path = path.substring(_propertyStoreRoot.length());

    return path;
  }
}
