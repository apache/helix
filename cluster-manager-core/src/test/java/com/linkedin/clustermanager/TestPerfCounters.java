package com.linkedin.clustermanager;


import org.testng.AssertJUnit;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.healthcheck.DefaultPerfCounters;

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
