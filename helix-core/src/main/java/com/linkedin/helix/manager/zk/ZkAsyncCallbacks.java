package com.linkedin.helix.manager.zk;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class ZkAsyncCallbacks
{
  private static Logger LOG = Logger.getLogger(ZkAsyncCallbacks.class);

  static class GetDataCallbackHandler extends DefaultCallback implements DataCallback
  {
    byte[] _data;
    Stat   _stat;

    @Override
    public void handle()
    {
      // TODO Auto-generated method stub
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)
    {
      if (rc == 0)
      {
        _data = data;
        _stat = stat;
      }
      callback(rc, path, ctx);
    }
  }

  static class SetDataCallbackHandler extends DefaultCallback implements StatCallback
  {
    Stat _stat;

    @Override
    public void handle()
    {
      // TODO Auto-generated method stub
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat)
    {
      if (rc == 0)
      {
        _stat = stat;
      }
      callback(rc, path, ctx);
    }
  }
  
  static class ExistsCallbackHandler extends DefaultCallback implements StatCallback
  {
    Stat _stat;

    @Override
    public void handle()
    {
      // TODO Auto-generated method stub
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat)
    {
      if (rc == 0)
      {
        _stat = stat;
      }
      callback(rc, path, ctx);
    }

  }

  static class CreateCallbackHandler extends DefaultCallback implements StringCallback
  {
    @Override
    public void processResult(int rc, String path, Object ctx, String name)
    {
      callback(rc, path, ctx);
    }

    @Override
    public void handle()
    {
      // TODO Auto-generated method stub
    }
  }

  static class DeleteCallbackHandler extends DefaultCallback implements VoidCallback
  {
    @Override
    public void processResult(int rc, String path, Object ctx)
    {
      callback(rc, path, ctx);
    }

    @Override
    public void handle()
    {
      // TODO Auto-generated method stub
    }

  }

  /**
   * Default callback for async api
   */
  static abstract class DefaultCallback
  {
    AtomicBoolean _lock = new AtomicBoolean(false);
    int           _rc   = -1;

    public void callback(int rc, String path, Object ctx)
    {
      if (rc != 0)
      {
        LOG.warn(this + ", rc:" + Code.get(rc) + ", path: " + path);
      }
      _rc = rc;
      handle();
      
      synchronized (_lock)
      {
        _lock.set(true);
        _lock.notify();
      }
    }

    public boolean waitForSuccess()
    {
      try
      {
        synchronized (_lock)
        {
          while (!_lock.get())
          {
            _lock.wait();
          }
        }
      }
      catch (InterruptedException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return true;
    }

    public int getRc()
    {
      return _rc;
    }

    abstract public void handle();
  }

}
