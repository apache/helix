package com.linkedin.helix.store.zk;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyChangeListener;

class ZkCallbackHandler<T> implements IZkChildListener, IZkDataListener
{
  private static Logger LOG = Logger.getLogger(ZkCallbackHandler.class);

  private final ZkClient _zkClient;
  private final ZKPropertyStore<T> _store;

  // listen on prefix and all its childs
  private final String _prefix;
  private final PropertyChangeListener<T> _listener;

  public ZkCallbackHandler(ZkClient client, ZKPropertyStore<T> store, String prefix,
                           PropertyChangeListener<T> listener)
  {
    _zkClient = client;
    _store = store;
    _prefix = prefix;
    _listener = listener;
  }

  @Override
  public void handleDataChange(String path, Object data) throws Exception
  {
    LOG.debug("Data changed @ " + path + " to " + data);
    String key = _store.getRelativePath(path);
    _listener.onPropertyChange(key);
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception
  {
    LOG.debug("Data deleted @ " + dataPath);
  }

  @Override
  public void handleChildChange(String path, List<String> currentChilds) throws Exception
  {
    LOG.debug("childs changed @ " + path + " to " + currentChilds);
    // System.out.println("childs changed @ " + path + " to " + currentChilds);


    if (currentChilds == null)
    {
      /**
       * When a node with a child change watcher is deleted
       * a child change is triggered on the deleted node
       * and in this case, the currentChilds is null
       */
      return;
    } else if (currentChilds.size() == 0)
    {
      String key = _store.getRelativePath(path);
      _listener.onPropertyChange(key);
    }
    else
    {
      // currentChilds.size() > 0
      for (String child : currentChilds)
      {
        String childPath = path.endsWith("/") ? path + child : path + "/" + child;
        _zkClient.subscribeDataChanges(childPath, this);
        _zkClient.subscribeChildChanges(childPath, this);

        // recursive call
        handleChildChange(childPath, _zkClient.getChildren(childPath));
      }
    }
  }
}
