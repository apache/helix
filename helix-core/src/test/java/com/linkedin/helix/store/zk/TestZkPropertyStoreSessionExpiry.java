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
package com.linkedin.helix.store.zk;

import java.util.Date;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyChangeListener;
import com.linkedin.helix.store.PropertyJsonSerializer;

public class TestZkPropertyStoreSessionExpiry extends ZkUnitTestBase
{
  private static final Logger LOG = Logger.getLogger(TestZkPropertyStoreSessionExpiry.class);

  private class TestPropertyChangeListener
  implements PropertyChangeListener<String>
  {
    public boolean _propertyChangeReceived = false;

    @Override
    public void onPropertyChange(String key)
    {
      // TODO Auto-generated method stub
      LOG.info("property change, " + key);
      _propertyChangeReceived = true;
    }
  }

	ZkClient _zkClient;

	@BeforeClass
	public void beforeClass()
	{
		_zkClient = new ZkClient(ZK_ADDR);
		_zkClient.setZkSerializer(new ZNRecordSerializer());
	}

	@AfterClass
	public void afterClass()
	{
		_zkClient.close();
	}


  @Test()
  public void testZkPropertyStoreSessionExpiry() throws Exception
  {
    LOG.info("START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

    PropertyJsonSerializer<String> serializer = new PropertyJsonSerializer<String>(String.class);

    ZkConnection zkConn = new ZkConnection(ZK_ADDR);

    final String propertyStoreRoot = "/" + getShortClassName();
    if (_zkClient.exists(propertyStoreRoot))
    {
      _zkClient.deleteRecursive(propertyStoreRoot);
    }

    ZKPropertyStore<String> zkPropertyStore 
      = new ZKPropertyStore<String>(new ZkClient(zkConn), serializer, propertyStoreRoot);

    zkPropertyStore.setProperty("/child1/grandchild1", "grandchild1");
    zkPropertyStore.setProperty("/child1/grandchild2", "grandchild2");

    TestPropertyChangeListener listener = new TestPropertyChangeListener();
    zkPropertyStore.subscribeForPropertyChange("", listener);

    listener._propertyChangeReceived = false;
    zkPropertyStore.setProperty("/child2/grandchild3", "grandchild3");
    Thread.sleep(100);
    AssertJUnit.assertEquals(listener._propertyChangeReceived, true);

    simulateSessionExpiry(zkConn);

    listener._propertyChangeReceived = false;
    zkPropertyStore.setProperty("/child2/grandchild4", "grandchild4");
    Thread.sleep(100);
    AssertJUnit.assertEquals(listener._propertyChangeReceived, true);

    LOG.info("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

  }
}
