package com.linkedin.clustermanager;

import java.io.File;
import java.io.IOException;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Base class to start stop ZK server Any test case that involves starting and
 * stopping the
 * 
 * @author kgopalak
 * 
 */
public class ZKBaseTest
{
  private static Logger _logger = Logger.getLogger(ZKBaseTest.class);
  protected ZkClient _zkClient;
  protected int _port = 2029;
  protected String _logDir = "/tmp/logs.zk.test";
  protected String _dataDir = "/tmp/dataDir.zk.test";
  protected ZkServer _zkServer;
  @BeforeClass
  public void startZookeeper()
  {
    try
    {
      FileUtils.deleteDirectory(new File(_dataDir));
      FileUtils.deleteDirectory(new File(_logDir));
    } catch (IOException e)
    {
      e.printStackTrace();
    }
    new File(_dataDir).delete();
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient)
      {
      }
    };
    _logger.info("Starting Zookeeper at localhost port:"+ _port );
    
    _zkServer = new ZkServer(_dataDir, _logDir, defaultNameSpace, _port);
    _zkServer.start();
  }
  
  @AfterClass
  public void stopZookeeper()
  {
    _logger.info("Shutting down Zookeeper at localhost port:"+ _port );
    if (_zkServer != null)
    {
      _zkServer.shutdown();
    }
  }
}
