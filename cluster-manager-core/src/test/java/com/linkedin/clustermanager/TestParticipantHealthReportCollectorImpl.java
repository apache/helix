package com.linkedin.clustermanager;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;

import com.linkedin.clustermanager.Mocks.MockHealthReportProvider;
import com.linkedin.clustermanager.Mocks.MockManager;
import com.linkedin.clustermanager.healthcheck.*;
public class TestParticipantHealthReportCollectorImpl {

	protected ParticipantHealthReportCollectorImpl _providerImpl;
	protected ClusterManager _manager;
	protected MockHealthReportProvider _mockProvider;
	
	 @BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		 _providerImpl = new ParticipantHealthReportCollectorImpl(new MockManager(), "instance_123");
		 _mockProvider = new MockHealthReportProvider();
	}
	
	 @Test (groups = {"unitTest"})
	  public void testStart() throws Exception
	  {
		 _providerImpl.start();
		 _providerImpl.start();
	  }
	 
	 @Test (groups = {"unitTest"})
	  public void testStop() throws Exception
	  {
		 _providerImpl.stop();
		 _providerImpl.stop();
	  }
	 
	 @Test (groups = {"unitTest"})
	 public void testAddProvider() throws Exception 
	 {
		 _providerImpl.removeHealthReportProvider(_mockProvider);
		 _providerImpl.addHealthReportProvider(_mockProvider);
		 _providerImpl.addHealthReportProvider(_mockProvider);
	 }
	 
	 @Test (groups = {"unitTest"})
	 public void testRemoveProvider() throws Exception
	 {
		 _providerImpl.addHealthReportProvider(_mockProvider);
		 _providerImpl.removeHealthReportProvider(_mockProvider);
		 _providerImpl.removeHealthReportProvider(_mockProvider);
	 }
}
