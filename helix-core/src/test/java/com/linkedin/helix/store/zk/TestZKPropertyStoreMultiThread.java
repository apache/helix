package com.linkedin.helix.store.zk;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyJsonComparator;
import com.linkedin.helix.store.PropertyJsonSerializer;

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
    for (Boolean result : results.values())
    {
      Assert.assertTrue(result.booleanValue());
    }

    System.out.println("END testUpdateUntilSucceed at" + new Date(System.currentTimeMillis()));
  }
}
