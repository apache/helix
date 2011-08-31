package com.linkedin.clustermanager;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
	/**
	 * Create the test case
	 * 
	 * @param testName
	 *          name of the test case
	 */
	public AppTest(String testName)
	{
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite(AppTest.class);
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp()
	{
		assertTrue(true);
	}

	public static void main(String[] args) throws Exception
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
		ZkServer server = new ZkServer(dataDir, logDir, defaultNameSpace, 2181);
		server.start();

		ZkClient client = new ZkClient("localhost:2181");
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

		server.shutdown();
		client.waitForKeeperState(KeeperState.Disconnected, 20000,
		    TimeUnit.MILLISECONDS);
		server.start();
		client.waitUntilConnected();

		Thread.sleep(1000);
		client.setZkSerializer(new BytesPushThroughSerializer());
		client.create("/test", new byte[0], CreateMode.EPHEMERAL);
		Thread.sleep(1000);

	}
}
