package org.apache.helix.zookeeper.impl.client;

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

import java.lang.management.ManagementFactory;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallMonitorContext;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.metric.ZkClientMonitor;
import org.apache.helix.zookeeper.zkclient.metric.ZkClientPathMonitor;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks.UNKNOWN_RET_CODE;

public class TestZkClientAsyncFailure extends ZkTestBase {
  private final String TEST_ROOT = String.format("/%s", getClass().getSimpleName());
  private final String NODE_PATH = TEST_ROOT + "/async";
  final String TEST_TAG = "test_tag";
  final String TEST_KEY = "test_key";
  final String TEST_INSTANCE = "test_instance";

  private org.apache.helix.zookeeper.zkclient.ZkClient _zkClient;
  private String _zkServerAddress;

  private final MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  private ZkClientMonitor _monitor;

  @BeforeClass
  public void beforeClass() throws JMException {
    _zkClient = _zkServerMap.values().iterator().next().getZkClient();
    _zkServerAddress = _zkClient.getServers();
    _zkClient.createPersistent(TEST_ROOT);

    _monitor = new ZkClientMonitor(TEST_TAG, TEST_KEY, TEST_INSTANCE, false, null);
    _monitor.register();
  }

  @AfterClass
  public void afterClass() {
    _monitor.unregister();
    _zkClient.deleteRecursively(TEST_ROOT);
    _zkClient.close();
  }

  private ObjectName buildObjectName(String tag, String key, String instance)
      throws MalformedObjectNameException {
    return ZkClientMonitor.getObjectName(tag, key, instance);
  }

  private ObjectName buildPathMonitorObjectName(String tag, String key, String instance,
      String path) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s,%s=%s", buildObjectName(tag, key, instance).toString(),
        ZkClientPathMonitor.MONITOR_PATH, path));
  }

  @Test
  public void testAsyncWrite() throws JMException {
    TestZkClientAsyncFailure.MockAsyncZkClient testZkClient =
        new TestZkClientAsyncFailure.MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      ObjectName instancesName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY, TEST_INSTANCE,
          ZkClientPathMonitor.PredefinedPath.Root.name());

      ZkAsyncCallbacks.SetDataCallbackHandler setCallback =
          new ZkAsyncCallbacks.SetDataCallbackHandler();
      Assert.assertEquals(setCallback.getRc(), UNKNOWN_RET_CODE);

      tmpRecord.setSimpleField("test", "data");

      // asyncSet should succeed because the return code is OK
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.asyncSetData(NODE_PATH, tmpRecord, -1, setCallback);
      setCallback.waitForSuccess();
      Assert.assertEquals(setCallback.getRc(), KeeperException.Code.OK.intValue());
      Assert.assertEquals(((ZNRecord) testZkClient.readData(NODE_PATH)).getSimpleField("test"),
          "data");
      Assert.assertEquals((long) _beanServer.getAttribute(instancesName,
          ZkClientPathMonitor.PredefinedMetricDomains.WriteFailureCounter.toString()), 0);

      // asyncSet should fail because the return code is APIERROR
      testZkClient.setAsyncCallRC(KeeperException.Code.APIERROR.intValue());
      testZkClient.asyncSetData(NODE_PATH, tmpRecord, -1, setCallback);
      setCallback.waitForSuccess();
      Assert.assertEquals(setCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      Assert.assertEquals((long) _beanServer.getAttribute(instancesName,
          ZkClientPathMonitor.PredefinedMetricDomains.WriteFailureCounter.toString()), 1);
    } finally {
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  @Test
  public void testAsyncRead() throws JMException {
    TestZkClientAsyncFailure.MockAsyncZkClient testZkClient =
        new TestZkClientAsyncFailure.MockAsyncZkClient(_zkServerAddress);
    try {
      ZNRecord tmpRecord = new ZNRecord("tmpRecord");
      tmpRecord.setSimpleField("foo", "bar");
      testZkClient.createPersistent(NODE_PATH, tmpRecord);

      ObjectName instancesName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY, TEST_INSTANCE,
          ZkClientPathMonitor.PredefinedPath.Root.name());

      ZkAsyncCallbacks.GetDataCallbackHandler getCallback =
          new ZkAsyncCallbacks.GetDataCallbackHandler();
      Assert.assertEquals(getCallback.getRc(), UNKNOWN_RET_CODE);

      // asyncGet should succeed because the return code is OK
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.asyncGetData(NODE_PATH, getCallback);
      getCallback.waitForSuccess();
      Assert.assertEquals(getCallback.getRc(), KeeperException.Code.OK.intValue());
      Assert.assertEquals((long) _beanServer.getAttribute(instancesName,
          ZkClientPathMonitor.PredefinedMetricDomains.ReadFailureCounter.toString()), 0);
      ZNRecord record = testZkClient.deserialize(getCallback._data, NODE_PATH);
      Assert.assertEquals(record.getSimpleField("foo"), "bar");

      // asyncGet should fail because the return code is APIERROR
      testZkClient.setAsyncCallRC(KeeperException.Code.APIERROR.intValue());
      testZkClient.asyncGetData(NODE_PATH, getCallback);
      getCallback.waitForSuccess();
      Assert.assertEquals(getCallback.getRc(), KeeperException.Code.APIERROR.intValue());
      Assert.assertEquals((long) _beanServer.getAttribute(instancesName,
          ZkClientPathMonitor.PredefinedMetricDomains.ReadFailureCounter.toString()), 1);
    } finally {
      testZkClient.setAsyncCallRC(KeeperException.Code.OK.intValue());
      testZkClient.close();
      _zkClient.delete(NODE_PATH);
    }
  }

  /**
   * Mock client to whitebox test async functionality.
   */
  class MockAsyncZkClient extends ZkClient {

    /**
     * If the specified return code is OK, call the real function.
     * Otherwise, trigger the callback with the specified RC without triggering the real ZK call.
     */
    private int _asyncCallRetCode = KeeperException.Code.OK.intValue();

    public MockAsyncZkClient(String zkAddress) {
      super(zkAddress);
      setZkSerializer(new ZNRecordSerializer());
    }

    public void setAsyncCallRC(int rc) {
      _asyncCallRetCode = rc;
    }

    @Override
    public void asyncSetData(String path, Object datat, int version,
        ZkAsyncCallbacks.SetDataCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncSetData(path, datat, version, cb);
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncCallMonitorContext(_monitor, 0, 0, false), null);
      }
    }

    @Override
    public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
      if (_asyncCallRetCode == KeeperException.Code.OK.intValue()) {
        super.asyncGetData(path, cb);
      } else {
        cb.processResult(_asyncCallRetCode, path,
            new ZkAsyncCallMonitorContext(_monitor, 0, 0, true), null, null);
      }
    }
  }
}
