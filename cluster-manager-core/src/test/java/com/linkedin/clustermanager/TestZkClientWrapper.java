package com.linkedin.clustermanager;

import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkClientWrapper extends ZKBaseTest
{
  @Test
  void testGetStat()
  {
    String path = "/tmp/getStatTest";
    _zkClient.deleteRecursive(path);

    Stat stat, newStat;
    stat = _zkClient.getStat(path);
    Assert.assertNull(stat);
    _zkClient.createPersistent(path, true);

    stat = _zkClient.getStat(path);
    Assert.assertNotNull(stat);

    newStat = _zkClient.getStat(path);
    Assert.assertEquals(stat, newStat);
    
    _zkClient.writeData(path, "Test".getBytes());
    newStat = _zkClient.getStat(path);
    Assert.assertNotSame(stat, newStat);
  }
}
