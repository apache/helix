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
package org.apache.helix.store.zk;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.PropertyChangeListener;
import org.apache.log4j.Logger;


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
//    } else if (currentChilds.size() == 0)
//    {
//      String key = _store.getRelativePath(path);
//      _listener.onPropertyChange(key);
    }
    else
    {
      String key = _store.getRelativePath(path);
      _listener.onPropertyChange(key);

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
