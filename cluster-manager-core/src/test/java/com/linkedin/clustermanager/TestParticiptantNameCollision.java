package com.linkedin.clustermanager;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestParticiptantNameCollision extends ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(TestParticiptantNameCollision.class);
  static final AtomicInteger _exceptionCounter = new AtomicInteger();
  
  @Test
  public void testParticiptantNameCollision() throws Exception
  {
    logger.info("RUN at " + new Date(System.currentTimeMillis()));
    
    // List<DummyProcessResult> results = new ArrayList<DummyProcessResult>();
    
    int i = 0;
    for (; i < 1; i++)
    {
      String instanceName = "localhost_" + (START_PORT + i);
      try
      {
        // the call fails on getClusterManagerForParticipant()
        // no threads start
        TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName, null);
      }
      catch (ClusterManagerException e)
      {
        _exceptionCounter.addAndGet(1);
        logger.info("exceptionCounter:" + _exceptionCounter.get());
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
    
    // Thread.sleep(40000);
  
    Assert.assertEquals(i, _exceptionCounter.get());
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }

  /*
  private static Thread startDummyProcess(final String[] args) 
  {
    Thread t = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          DummyProcess.main(args);
        } 
        catch (ClusterManagerException e)
        {
          _exceptionCounter.addAndGet(1);
          logger.info("exceptionCounter:" + _exceptionCounter.get());
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });
    t.start();

    return t;
  }

  private static String[] createArgs(String str)
  {
    String[] split = str.split("[ ]+");
    System.out.println(Arrays.toString(split));
    return split;
  }
  */
}
