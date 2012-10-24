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

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.PropertyJsonComparator;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.store.zk.ZKPropertyStore;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZKPropertyStoreMultiThread extends ZkUnitTestBase
{
  private static final Logger LOG = Logger.getLogger(TestZKPropertyStoreMultiThread.class);

  private final String _root = "/" + getShortClassName();
  private final ZNRecord _record = new ZNRecord("key1");


  @BeforeClass()
  public void beforeClass()
  {
    if (_gZkClient.exists(_root))
    {
      _gZkClient.deleteRecursive(_root);
    }
  }

  public class TestCallableCAS implements Callable<Boolean>
  {
    @Override
    public Boolean call()
    {
      try
      {
        ZKPropertyStore<ZNRecord> store
          = new ZKPropertyStore<ZNRecord>(new ZkClient(ZK_ADDR),
                                          new PropertyJsonSerializer<ZNRecord>(ZNRecord.class),
                                          _root);
        PropertyJsonComparator<ZNRecord> comparator = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
        long id = Thread.currentThread().getId();

        store.setProperty("key1", _record);

        boolean success;
        do
        {
          ZNRecord current = store.getProperty("key1");
          ZNRecord update = new ZNRecord(current);
          update.setSimpleField("thread_" + id, "simpleValue");

          success = store.compareAndSet("key1", current, update, comparator);
        } while (!success);

        return Boolean.TRUE;
      }
      catch (Exception e)
      {
        LOG.error(e);
        return Boolean.FALSE;
      }
    }
  }

  @Test ()
  public void testCmpAndSet()
  {
    System.out.println("START testCmpAndSet at" + new Date(System.currentTimeMillis()));

    Map<String, Boolean> results = TestHelper.<Boolean>startThreadsConcurrently(5, new TestCallableCAS(), 10);
    Assert.assertEquals(results.size(), 5);
    for (Boolean result : results.values())
    {
      Assert.assertTrue(result.booleanValue());
    }

    System.out.println("END testCmpAndSet at" + new Date(System.currentTimeMillis()));
  }

  private class TestUpdater implements DataUpdater<ZNRecord>
  {

    @Override
    public ZNRecord update(ZNRecord current)
    {
      long id = Thread.currentThread().getId();

      current.setSimpleField("thread_" + id, "simpleValue");
      return current;
    }

  }

  public class TestCallableUpdate implements Callable<Boolean>
  {
    @Override
    public Boolean call()
    {
      try
      {
        ZKPropertyStore<ZNRecord> store
          = new ZKPropertyStore<ZNRecord>(new ZkClient(ZK_ADDR),
                                          new PropertyJsonSerializer<ZNRecord>(ZNRecord.class),
                                          _root);

        store.setProperty("key2", _record);
        store.updatePropertyUntilSucceed("key2", new TestUpdater());

        return Boolean.TRUE;
      }
      catch (Exception e)
      {
        LOG.error(e);
        return Boolean.FALSE;
      }
    }
  }

  @Test()
  public void testUpdateUntilSucceed()
  {
    System.out.println("START testUpdateUntilSucceed at" + new Date(System.currentTimeMillis()));

    Map<String, Boolean> results = TestHelper.<Boolean>startThreadsConcurrently(5, new TestCallableUpdate(), 10);
    Assert.assertEquals(results.size(), 5);
    for (Boolean result : results.values())
    {
      Assert.assertTrue(result.booleanValue());
    }

    System.out.println("END testUpdateUntilSucceed at" + new Date(System.currentTimeMillis()));
  }
}
