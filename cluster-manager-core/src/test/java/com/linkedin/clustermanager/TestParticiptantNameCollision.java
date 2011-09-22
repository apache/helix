package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.mock.storage.DummyProcess;

public class TestParticiptantNameCollision extends ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(TestParticiptantNameCollision.class);
  static final AtomicInteger _exceptionCounter = new AtomicInteger();
  
  @Test
  public void testParticiptantNameCollision() throws Exception
  {
    logger.info("RUN at " + new Date(System.currentTimeMillis()));
    
    List<Thread> tList = new ArrayList<Thread>();
    
    tList.add(startDummyProcess(createArgs("-zkSvr " + ZK_ADDR + " -cluster " + CLUSTER_NAME + 
                                           " -host localhost -port 12919")));
    tList.add(startDummyProcess(createArgs("-zkSvr " + ZK_ADDR + " -cluster " + CLUSTER_NAME + 
                                           " -host localhost -port 12920")));

    Thread.sleep(40000);
    Assert.assertEquals(2, _exceptionCounter.get());
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }

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
}
