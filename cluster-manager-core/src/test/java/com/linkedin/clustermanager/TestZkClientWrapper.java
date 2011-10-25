package com.linkedin.clustermanager;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkClientWrapper extends ZkUnitTestBase
{
    @Test (groups = {"unitTest"})
	void testGetStat()
	{
		String path = "/tmp/getStatTest";
		_zkClient.deleteRecursive(path);

		Stat stat, newStat;
		stat = _zkClient.getStat(path);
		AssertJUnit.assertNull(stat);
		_zkClient.createPersistent(path, true);

		stat = _zkClient.getStat(path);
		AssertJUnit.assertNotNull(stat);

		newStat = _zkClient.getStat(path);
		AssertJUnit.assertEquals(stat, newStat);

		_zkClient.writeData(path, "Test".getBytes());
		newStat = _zkClient.getStat(path);
		AssertJUnit.assertNotSame(stat, newStat);
	}

    @Test (groups = {"unitTest"})
	void testSessioExpire()
	{
		IZkStateListener listener = new IZkStateListener()
		{

			@Override
			public void handleStateChanged(KeeperState state) throws Exception
			{
				System.out.println("In Old connection New state " + state);
			}

			@Override
			public void handleNewSession() throws Exception
			{
				System.out.println("In Old connection New session");
			}
		};
		_zkClient.subscribeStateChanges(listener);
		ZkConnection connection = ((ZkConnection) _zkClient.getConnection());
		ZooKeeper zookeeper = connection.getZookeeper();
		System.out.println("old sessionId= " + zookeeper.getSessionId());
		try
		{
			Watcher watcher = new Watcher(){
				@Override
        public void process(WatchedEvent event)
        {
					System.out.println("In New connection In process event:"+ event);
        }
			};
			ZooKeeper newZookeeper = new ZooKeeper(connection.getServers(),
			    zookeeper.getSessionTimeout(), watcher , zookeeper.getSessionId(),
			    zookeeper.getSessionPasswd());
			Thread.sleep(3000);
			System.out.println("New sessionId= " + newZookeeper.getSessionId());
			Thread.sleep(3000);
			newZookeeper.close();
			Thread.sleep(10000);
			connection = ((ZkConnection) _zkClient.getConnection());
			zookeeper = connection.getZookeeper();
			System.out.println("After session expiry sessionId= " + zookeeper.getSessionId());
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
