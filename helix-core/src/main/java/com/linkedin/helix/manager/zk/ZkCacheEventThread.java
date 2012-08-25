package com.linkedin.helix.manager.zk;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.log4j.Logger;

// copy from ZkEventThread
public class ZkCacheEventThread extends Thread
{

  private static final Logger          LOG      =
                                                    Logger.getLogger(ZkCacheEventThread.class);
  private final BlockingQueue<ZkCacheEvent> _events  = new LinkedBlockingQueue<ZkCacheEvent>();
  private static AtomicInteger         _eventId = new AtomicInteger(0);

  static abstract class ZkCacheEvent
  {

    private final String _description;

    public ZkCacheEvent(String description)
    {
      _description = description;
    }

    public abstract void run() throws Exception;

    @Override
    public String toString()
    {
      return "ZkCacheEvent[" + _description + "]";
    }
  }

  ZkCacheEventThread(String name)
  {
    setDaemon(true);
    setName("ZkCache-EventThread-" + getId() + "-" + name);
  }

  @Override
  public void run()
  {
    LOG.info("Starting ZkCache event thread.");
    try
    {
      while (!isInterrupted())
      {
        ZkCacheEvent zkEvent = _events.take();
        int eventId = _eventId.incrementAndGet();
        LOG.debug("Delivering event #" + eventId + " " + zkEvent);
        try
        {
          zkEvent.run();
        }
        catch (InterruptedException e)
        {
          interrupt();
        }
        catch (ZkInterruptedException e)
        {
          interrupt();
        }
        catch (Throwable e)
        {
          LOG.error("Error handling event " + zkEvent, e);
        }
        LOG.debug("Delivering event #" + eventId + " done");
      }
    }
    catch (InterruptedException e)
    {
      LOG.info("Terminate ZkClient event thread.");
    }
  }

  public void send(ZkCacheEvent event)
  {
    if (!isInterrupted())
    {
      LOG.debug("New event: " + event);
      _events.add(event);
    }
  }
}
