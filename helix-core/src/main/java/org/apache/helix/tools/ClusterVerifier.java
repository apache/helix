package org.apache.helix.tools;

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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;

public abstract class ClusterVerifier implements IZkChildListener, IZkDataListener {
  private static Logger LOG = Logger.getLogger(ClusterVerifier.class);

  protected final ZkClient _zkclient;
  protected final String _clusterName;
  protected final HelixDataAccessor _accessor;
  protected final PropertyKey.Builder _keyBuilder;
  private CountDownLatch _countdown;

  static class ClusterVerifyTrigger {
    final PropertyKey _triggerKey;
    final boolean _triggerOnChildDataChange;

    public ClusterVerifyTrigger(PropertyKey triggerKey, boolean triggerOnChildDataChange) {
      _triggerKey = triggerKey;
      _triggerOnChildDataChange = triggerOnChildDataChange;
    }
  }

  public ClusterVerifier(ZkClient zkclient, String clusterName) {
    _zkclient = zkclient;
    _clusterName = clusterName;
    _accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkclient));
    _keyBuilder = _accessor.keyBuilder();
  }

  public boolean verifyByCallback(long timeout, List<ClusterVerifyTrigger> triggers) {
    _countdown = new CountDownLatch(1);

    for (ClusterVerifyTrigger trigger : triggers) {
      String path = trigger._triggerKey.getPath();
      _zkclient.subscribeChildChanges(path, this);
      if (trigger._triggerOnChildDataChange) {
        List<String> childs = _zkclient.getChildren(path);
        for (String child : childs) {
          String childPath = String.format("%s/%s", path, child);
          _zkclient.subscribeDataChanges(childPath, this);
        }
      }
    }

    boolean success = false;
    try {
      success = verify();
      if (!success) {

        success = _countdown.await(timeout, TimeUnit.MILLISECONDS);
        if (!success) {
          // make a final try if timeout
          success = verify();
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in verifier", e);
    }

    // clean up
    _zkclient.unsubscribeAll();

    return success;
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {
    boolean success = verify();
    if (success) {
      _countdown.countDown();
    }
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
    _zkclient.unsubscribeDataChanges(dataPath, this);
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
    for (String child : currentChilds) {
      String childPath = String.format("%s/%s", parentPath, child);
      _zkclient.subscribeDataChanges(childPath, this);
    }

    boolean success = verify();
    if (success) {
      _countdown.countDown();
    }
  }

  public boolean verifyByPolling(long timeout) {
    try {
      long start = System.currentTimeMillis();
      boolean success;
      do {
        success = verify();
        if (success) {
          return true;
        }
        TimeUnit.MILLISECONDS.sleep(500);
      } while ((System.currentTimeMillis() - start) <= timeout);
    } catch (Exception e) {
      LOG.error("Exception in verifier", e);
    }
    return false;
  }

  /**
   * verify
   * @return
   * @throws Exception
   */
  public abstract boolean verify() throws Exception;
}
