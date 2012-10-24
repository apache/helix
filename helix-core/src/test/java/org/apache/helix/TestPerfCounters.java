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


import org.apache.helix.healthcheck.DefaultPerfCounters;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestPerfCounters {

	final String INSTANCE_NAME = "instance_123";
    final long AVAILABLE_CPUS = 1;
    final long FREE_PHYSICAL_MEMORY = 2;
    final long FREE_JVM_MEMORY = 3;
    final long TOTAL_JVM_MEMORY = 4;
    final double AVERAGE_SYSTEM_LOAD = 5;

	DefaultPerfCounters _perfCounters;

	@BeforeTest ()
	public void setup()
	{
		_perfCounters = new DefaultPerfCounters(INSTANCE_NAME, AVAILABLE_CPUS,
				FREE_PHYSICAL_MEMORY, FREE_JVM_MEMORY, TOTAL_JVM_MEMORY,
				AVERAGE_SYSTEM_LOAD);
	}

	 @Test ()
	 public void testGetAvailableCpus()
	 {
		 AssertJUnit.assertEquals(AVAILABLE_CPUS,_perfCounters.getAvailableCpus());
	 }

	 @Test ()
	 public void testGetAverageSystemLoad()
	 {
		 AssertJUnit.assertEquals(AVERAGE_SYSTEM_LOAD,_perfCounters.getAverageSystemLoad());
	 }

	 @Test ()
	 public void testGetTotalJvmMemory()
	 {
		 AssertJUnit.assertEquals(TOTAL_JVM_MEMORY,_perfCounters.getTotalJvmMemory());
	 }

	 @Test ()
	 public void testGetFreeJvmMemory()
	 {
		 AssertJUnit.assertEquals(FREE_JVM_MEMORY,_perfCounters.getFreeJvmMemory());
	 }

	 @Test ()
	 public void testGetFreePhysicalMemory()
	 {
		 AssertJUnit.assertEquals(FREE_PHYSICAL_MEMORY,_perfCounters.getFreePhysicalMemory());
	 }
}
