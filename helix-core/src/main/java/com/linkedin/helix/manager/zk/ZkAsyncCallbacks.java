package com.linkedin.helix.manager.zk;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.data.Stat;

public class ZkAsyncCallbacks
{
  static class SetDataCallbackHandler extends DefaultCallback implements StatCallback
  {
    @Override
    public void handle()
    {
      // TODO Auto-generated method stub
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat)
    {
      // TODO Auto-generated method stub
      callback(rc, ctx);
    }

  }
  
  static class CreateCallbackHandler extends DefaultCallback implements StringCallback
  {
    @Override
    public void processResult(int rc, String path, Object ctx, String name)
    {
      // TODO Auto-generated method stub
      callback(rc, ctx);
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
    final AtomicBoolean _success = new AtomicBoolean(false);

    public void callback(int rc, Object ctx)
    {
      synchronized (_success)
      {
        if (rc == 0)
        {
          _success.set(true);
        }
        handle();
        _success.notify();
      }
    }

    public boolean waitForSuccess()
    {
      try
      {
        while (!_success.get())
        {
          synchronized (_success)
          {
            _success.wait();
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

    public boolean getSuccess()
    {
      return _success.get();
    }

    abstract public void handle();
  }

}
