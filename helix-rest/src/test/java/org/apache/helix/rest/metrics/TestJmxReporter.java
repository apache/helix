package org.apache.helix.rest.metrics;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("rawtypes")
public class TestJmxReporter {
  private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
  private final String domain = UUID.randomUUID().toString().replaceAll("[{\\-}]", "");
  private MetricRegistry registry = new MetricRegistry();

  private JmxReporter reporter = JmxReporter.forRegistry(registry)
      .registerWith(mBeanServer)
        .inDomain(domain)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .filter(MetricFilter.ALL)
        .build();

  private final Gauge gauge = mock(Gauge.class);
  private final Counter counter = mock(Counter.class);
  private final Histogram histogram = mock(Histogram.class);
  private final Meter meter = mock(Meter.class);
  private final Timer timer = mock(Timer.class);

  @BeforeMethod
  public void setUp() {
    when(gauge.getValue()).thenReturn(1);

    when(counter.getCount()).thenReturn(100L);

    when(histogram.getCount()).thenReturn(1L);

    final Snapshot hSnapshot = mock(Snapshot.class);
    when(hSnapshot.getMax()).thenReturn(2L);
    when(hSnapshot.getMean()).thenReturn(3.0);
    when(hSnapshot.getMin()).thenReturn(4L);
    when(hSnapshot.getStdDev()).thenReturn(5.0);
    when(hSnapshot.getMedian()).thenReturn(6.0);
    when(hSnapshot.get75thPercentile()).thenReturn(7.0);
    when(hSnapshot.get95thPercentile()).thenReturn(8.0);
    when(hSnapshot.get98thPercentile()).thenReturn(9.0);
    when(hSnapshot.get99thPercentile()).thenReturn(10.0);
    when(hSnapshot.get999thPercentile()).thenReturn(11.0);
    when(hSnapshot.size()).thenReturn(1);

    when(histogram.getSnapshot()).thenReturn(hSnapshot);

    when(meter.getCount()).thenReturn(1L);
    when(meter.getMeanRate()).thenReturn(2.0);
    when(meter.getOneMinuteRate()).thenReturn(3.0);
    when(meter.getFiveMinuteRate()).thenReturn(4.0);
    when(meter.getFifteenMinuteRate()).thenReturn(5.0);

    when(timer.getCount()).thenReturn(1L);
    when(timer.getMeanRate()).thenReturn(2.0);
    when(timer.getOneMinuteRate()).thenReturn(3.0);
    when(timer.getFiveMinuteRate()).thenReturn(4.0);
    when(timer.getFifteenMinuteRate()).thenReturn(5.0);

    final Snapshot tSnapshot = mock(Snapshot.class);
    when(tSnapshot.getMax()).thenReturn(TimeUnit.MILLISECONDS.toNanos(100));
    when(tSnapshot.getMean()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(200));
    when(tSnapshot.getMin()).thenReturn(TimeUnit.MILLISECONDS.toNanos(300));
    when(tSnapshot.getStdDev()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(400));
    when(tSnapshot.getMedian()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(500));
    when(tSnapshot.get75thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(600));
    when(tSnapshot.get95thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(700));
    when(tSnapshot.get98thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(800));
    when(tSnapshot.get99thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(900));
    when(tSnapshot.get999thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(1000));
    when(tSnapshot.size()).thenReturn(1);

    when(timer.getSnapshot()).thenReturn(tSnapshot);

    registry.register("gauge", gauge);
    registry.register("test.counter", counter);
    registry.register("test.histogram", histogram);
    registry.register("test.meter", meter);
    registry.register("test.another.timer", timer);

    reporter.start();
  }

  @AfterMethod
  public void tearDown() {
    reporter.stop();
    registry.removeMatching(MetricFilter.ALL);
  }

  @Test
  public void registersMBeansForGauges() throws Exception {
    final AttributeList attributes = getAttributes("gauges", "gauge", "Value", "Number");

    Assert.assertEquals(values(attributes), ImmutableMap.of("Value", 1, "Number", 1));
  }

  @Test
  public void registersMBeansForCounters() throws Exception {
    final AttributeList attributes = getAttributes("counters", "test.counter", "Count");

    Assert.assertEquals(values(attributes), ImmutableMap.of("Count", 100L));
  }

  @Test
  public void registersMBeansForHistograms() throws Exception {
    final AttributeList attributes = getAttributes("histograms", "test.histogram",
        "Count",
        "Max",
        "Mean",
        "Min",
        "StdDev",
        "50thPercentile",
        "75thPercentile",
        "95thPercentile",
        "98thPercentile",
        "99thPercentile",
        "999thPercentile",
        "SnapshotSize");

    Map<String, Object> expectedMap = new HashMap<String, Object>() {{
      put("Count", 1L);
      put("Max", 2L);
      put("Mean", 3.0);
      put("Min", 4L);
      put("StdDev", 5.0);
      put("50thPercentile", 6.0);
      put("75thPercentile", 7.0);
      put("95thPercentile", 8.0);
      put("98thPercentile", 9.0);
      put("99thPercentile", 10.0);
      put("999thPercentile", 11.0);
      put("SnapshotSize", 1L);
    }};

    Assert.assertEquals(values(attributes), expectedMap);
  }

  @Test
  public void registersMBeansForMeters() throws Exception {
    final AttributeList attributes = getAttributes("meters", "test.meter",
        "Count",
        "MeanRate",
        "OneMinuteRate",
        "FiveMinuteRate",
        "FifteenMinuteRate",
        "RateUnit");

    Map<String, Object> expectedMap = new HashMap<String, Object>(){{
      put("Count", 1L);
      put("MeanRate", 2.0);
      put("OneMinuteRate", 3.0);
      put("FiveMinuteRate", 4.0);
      put("FifteenMinuteRate", 5.0);
      put("RateUnit", "events/second");
    }};

    Assert.assertEquals(values(attributes), expectedMap);
  }

  @Test
  public void registersMBeansForTimers() throws Exception {
    final AttributeList attributes = getAttributes("timers", "test.another.timer",
        "Count",
        "MeanRate",
        "OneMinuteRate",
        "FiveMinuteRate",
        "FifteenMinuteRate",
        "Max",
        "Mean",
        "Min",
        "StdDev",
        "50thPercentile",
        "75thPercentile",
        "95thPercentile",
        "98thPercentile",
        "99thPercentile",
        "999thPercentile",
        "RateUnit",
        "DurationUnit");

    Map<String, Object> expectedMap = new HashMap<String, Object>() {{
      put("Count", 1L);
      put("MeanRate", 2.0);
      put("OneMinuteRate", 3.0);
      put("FiveMinuteRate", 4.0);
      put("FifteenMinuteRate", 5.0);
      put("Max", 100.0);
      put("Mean", 200.0);
      put("Min", 300.0);
      put("StdDev", 400.0);
      put("50thPercentile", 500.0);
      put("75thPercentile", 600.0);
      put("95thPercentile", 700.0);
      put("98thPercentile", 800.0);
      put("99thPercentile", 900.0);
      put("999thPercentile", 1000.0);
      put("RateUnit", "events/second");
      put("DurationUnit", "milliseconds");
    }};

    Assert.assertEquals(values(attributes), expectedMap);
  }

  @Test
  public void cleansUpAfterItselfWhenStopped() throws Exception {
    reporter.stop();

    try {
      getAttributes("gauges", "gauge", "Value", "Number");
      Assert.fail("Should expect " + InstanceNotFoundException.class.getSimpleName());
    } catch (InstanceNotFoundException expected) {
      // Expected
    }
  }

  @Test
  public void objectNameModifyingMBeanServer() throws Exception {
    MBeanServer mockedMBeanServer = mock(MBeanServer.class);

    // overwrite the objectName
    when(mockedMBeanServer.registerMBean(any(Object.class), any(ObjectName.class)))
        .thenReturn(new ObjectInstance("DOMAIN:key=value", "className"));

    MetricRegistry testRegistry = new MetricRegistry();
    JmxReporter testJmxReporter = JmxReporter.forRegistry(testRegistry)
        .registerWith(mockedMBeanServer)
        .inDomain(domain)
        .build();

    testJmxReporter.start();

    // should trigger a registerMBean
    testRegistry.timer("test");

    // should trigger an unregisterMBean with the overwritten objectName = "DOMAIN:key=value"
    testJmxReporter.stop();

    verify(mockedMBeanServer).unregisterMBean(new ObjectName("DOMAIN:key=value"));
  }

  @Test
  public void testJmxMetricNameWithAsterisk() {
    MetricRegistry metricRegistry = new MetricRegistry();
    JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
    jmxReporter.start();
    metricRegistry.counter("test*");
    jmxReporter.stop();
  }

  @Test
  public void testJmxObjectNameWithMultiKeyProperties() throws JMException, InterruptedException {
    MetricRegistry metricRegistry = new MetricRegistry();
    Hashtable<String, String> keyProperties = new Hashtable<String, String>(){{
      put("key1", "value1");
      put("key2", "value2");
    }};

    JmxReporter testJmxReporter = JmxReporter.forRegistry(metricRegistry)
        .inDomain(domain)
        .withKeyProperties(keyProperties)
        .build();
    testJmxReporter.start();

    String name = TestHelper.getTestMethodName();
    metricRegistry.counter(name);
    keyProperties.put("name", name);

    Assert.assertTrue(mBeanServer.isRegistered(new ObjectName(domain, keyProperties)));

    testJmxReporter.stop();
  }

  private AttributeList getAttributes(String type, String name, String... attributeNames)
      throws JMException {
    Hashtable<String, String> keyProperties = new Hashtable<String, String>() {{
      put("name", name);
      put("type", type);
    }};
    ObjectName n = new ObjectName(this.domain, keyProperties);
    return mBeanServer.getAttributes(n, attributeNames);
  }

  private SortedMap<String, Object> values(AttributeList attributes) {
    final TreeMap<String, Object> values = new TreeMap<>();
    for (Object o : attributes) {
      final Attribute attribute = (Attribute) o;
      values.put(attribute.getName(), attribute.getValue());
    }
    return values;
  }
}
