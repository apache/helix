package org.apache.helix.monitoring.mbeans;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.PropertyType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRoutingTableProviderMonitor {

  private MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  private final String TEST_CLUSTER = "test_cluster";

  private ObjectName buildObjectName(PropertyType type, String cluster)
      throws MalformedObjectNameException {
    return MBeanRegistrar.buildObjectName(MonitorDomainNames.RoutingTableProvider.name(),
        RoutingTableProviderMonitor.CLUSTER_KEY, cluster, RoutingTableProviderMonitor.DATA_TYPE_KEY,
        type.name());
  }

  private ObjectName buildObjectName(PropertyType type, String cluster, int num)
      throws MalformedObjectNameException {
    ObjectName objectName = buildObjectName(type, cluster);
    if (num > 0) {
      return new ObjectName(String
          .format("%s,%s=%s", objectName.toString(), MBeanRegistrar.DUPLICATE,
              String.valueOf(num)));
    } else {
      return objectName;
    }
  }

  @Test
  public void testMBeanRegisteration() throws JMException {
    Set<RoutingTableProviderMonitor> monitors = new HashSet<>();
    for (PropertyType type : PropertyType.values()) {
      monitors.add(new RoutingTableProviderMonitor(type, TEST_CLUSTER).register());
      Assert.assertTrue(_beanServer.isRegistered(buildObjectName(type, TEST_CLUSTER)));
    }

    for (PropertyType type : PropertyType.values()) {
      monitors.add(new RoutingTableProviderMonitor(type, TEST_CLUSTER).register());
      Assert.assertTrue(_beanServer.isRegistered(buildObjectName(type, TEST_CLUSTER, 1)));
    }

    for (PropertyType type : PropertyType.values()) {
      monitors.add(new RoutingTableProviderMonitor(type, TEST_CLUSTER).register());
      Assert.assertTrue(_beanServer.isRegistered(buildObjectName(type, TEST_CLUSTER, 2)));
    }

    // Un-register all monitors
    for (RoutingTableProviderMonitor monitor : monitors) {
      monitor.unregister();
    }

    for (PropertyType type : PropertyType.values()) {
      Assert.assertFalse(_beanServer.isRegistered(buildObjectName(type, TEST_CLUSTER)));
      Assert.assertFalse(_beanServer.isRegistered(buildObjectName(type, TEST_CLUSTER, 1)));
      Assert.assertFalse(_beanServer.isRegistered(buildObjectName(type, TEST_CLUSTER, 2)));
    }
  }

  @Test
  public void testMetrics() throws JMException, InterruptedException {
    PropertyType type = PropertyType.EXTERNALVIEW;
    RoutingTableProviderMonitor monitor = new RoutingTableProviderMonitor(type, TEST_CLUSTER);
    monitor.register();
    ObjectName name = buildObjectName(type, TEST_CLUSTER);

    monitor.increaseCallbackCounters(10);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "CallbackCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "EventQueueSizeGauge"), 10);
    monitor.increaseCallbackCounters(15);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "CallbackCounter"), 2);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "EventQueueSizeGauge"), 15);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "DataRefreshLatencyGauge.Max"), 0);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "DataRefreshCounter"), 0);
    // StatePropagationLatencyGauge only apply for current state
    Assert.assertEquals(_beanServer.getAttribute(name, "StatePropagationLatencyGauge.Max"), null);

    long startTime = System.currentTimeMillis();
    Thread.sleep(5);
    monitor.increaseDataRefreshCounters(startTime);
    long latency = (long) _beanServer.getAttribute(name, "DataRefreshLatencyGauge.Max");
    Assert.assertTrue(latency >= 5 && latency <= System.currentTimeMillis() - startTime);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "DataRefreshCounter"), 1);

    monitor.increaseDataRefreshCounters(startTime);
    long newLatency = (long) _beanServer.getAttribute(name, "DataRefreshLatencyGauge.Max");
    Assert.assertTrue(newLatency >= latency);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "DataRefreshCounter"), 2);

    monitor.unregister();
  }

  public void testCurrentStateMetrics() throws JMException, InterruptedException {
    PropertyType type = PropertyType.CURRENTSTATES;
    RoutingTableProviderMonitor monitor = new RoutingTableProviderMonitor(type, TEST_CLUSTER);
    monitor.register();
    ObjectName name = buildObjectName(type, TEST_CLUSTER);

    monitor.increaseCallbackCounters(10);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "StatePropagationLatencyGauge.Max"), 0);

    monitor.recordStatePropagationLatency(5);
    long statelatency = (long) _beanServer.getAttribute(name, "StatePropagationLatencyGauge.Max");
    Assert.assertEquals(statelatency, 5);
    monitor.recordStatePropagationLatency(10);
    statelatency = (long) _beanServer.getAttribute(name, "StatePropagationLatencyGauge.Max");
    Assert.assertEquals(statelatency, 10);

    monitor.unregister();
  }
}
