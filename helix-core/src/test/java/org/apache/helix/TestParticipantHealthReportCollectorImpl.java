/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix;

import org.apache.helix.HelixManager;
import org.apache.helix.Mocks.MockHealthReportProvider;
import org.apache.helix.Mocks.MockManager;
import org.apache.helix.healthcheck.*;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;

public class TestParticipantHealthReportCollectorImpl {

	protected ParticipantHealthReportCollectorImpl _providerImpl;
	protected HelixManager _manager;
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
