package com.linkedin.clustermanager;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import com.linkedin.clustermanager.model.Message;

/**
 * Unit test for simple App.
 */
public class AppTest
{
	/**
	 * Create the test case
	 * 
	 * @param testName
	 *          name of the test case
	 */
	public AppTest(String testName)
	{
	}

	private static void testChrootWithZkClient() throws Exception
	{
		ZkClient client = new ZkClient("localhost:2181/foo");
		IZkStateListener stateChangeListener = new IZkStateListener()
		{

			@Override
			public void handleStateChanged(KeeperState state) throws Exception
			{
				System.out
				    .println("AppTest.main(...).new IZkStateListener() {...}.handleStateChanged()"
				        + state);
			}

			@Override
			public void handleNewSession() throws Exception
			{
				System.out
				    .println("AppTest.main(...).new IZkStateListener() {...}.handleNewSession()");
			}
		};
		client.subscribeStateChanges(stateChangeListener);
		boolean waitUntilConnected = client.waitUntilConnected(10000,
		    TimeUnit.MILLISECONDS);
		System.out.println("Connected " + waitUntilConnected);
		client.waitForKeeperState(KeeperState.Disconnected, 20000,
		    TimeUnit.MILLISECONDS);
		// server.start();
		client.waitUntilConnected();
		Thread.currentThread().join();
	}

	private static void testChroot() throws Exception
	{
		Watcher watcher = new Watcher()
		{
			@Override
			public void process(WatchedEvent event)
			{
				System.out.println("Event:" + event);
			}
		};
		ZooKeeper zk = new ZooKeeper("localhost:2181/foo", 6000, watcher);
		// uncommenting this line will not cause infinite connect/disconnect
		// zk.create("/", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		zk.exists("/", true);
		System.out
		    .println("Stop the server and restart it when you see this message");
		Thread.currentThread().join();
	}

	private static void testZKClient() throws InterruptedException
	{
		IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
		{
			@Override
			public void createDefaultNameSpace(ZkClient zkClient)
			{
			}
		};
		String dataDir = "/tmp/dataDir";
		String logDir = "/tmp/logDir";
		// ZkServer server = new ZkServer(dataDir, logDir, defaultNameSpace, 2181);
		// server.start();

		ZkClient client = new ZkClient("localhost:2181/foo");
		IZkStateListener stateChangeListener = new IZkStateListener()
		{

			@Override
			public void handleStateChanged(KeeperState state) throws Exception
			{
				System.out
				    .println("AppTest.main(...).new IZkStateListener() {...}.handleStateChanged()"
				        + state);
			}

			@Override
			public void handleNewSession() throws Exception
			{
				System.out
				    .println("AppTest.main(...).new IZkStateListener() {...}.handleNewSession()");
			}
		};
		client.subscribeStateChanges(stateChangeListener);
		boolean waitUntilConnected = client.waitUntilConnected(10000,
		    TimeUnit.MILLISECONDS);
		System.out.println("Connected " + waitUntilConnected);
		IZkChildListener listener1 = new IZkChildListener()
		{

			@Override
			public void handleChildChange(String parentPath,
			    List<String> currentChilds) throws Exception
			{
				System.out.println("listener 1 Change at path:" + parentPath);
			}
		};
		IZkChildListener listener2 = new IZkChildListener()
		{
			@Override
			public void handleChildChange(String parentPath,
			    List<String> currentChilds) throws Exception
			{
				System.out.println("listener2 Change at path:" + parentPath);
			}
		};

		client.subscribeChildChanges("/", listener1);
		client.subscribeChildChanges("/foo", listener2);

		// server.shutdown();
		client.waitForKeeperState(KeeperState.Disconnected, 20000,
		    TimeUnit.MILLISECONDS);
		// server.start();
		client.waitUntilConnected();

		Thread.sleep(1000);
		client.setZkSerializer(new BytesPushThroughSerializer());
		client.create("/test", new byte[0], CreateMode.EPHEMERAL);
		Thread.sleep(1000);
	}

	public static void main(String[] args) throws Exception
	{
	  Message message = new Message();
	  message.setFromState("asdasd");
	  Message clone = new Message(new ZNRecord(message.getRecord()));
	  clone.setFromState("newstate");
	  System.out.println(message.getFromState());
		//testChroot();
		// testZKClient();
		// testChrootWithZkClient();

	}
}
