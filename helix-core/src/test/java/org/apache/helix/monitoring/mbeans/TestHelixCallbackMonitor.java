package org.apache.helix.monitoring.mbeans;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.HelixConstants;
import org.apache.helix.InstanceType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixCallbackMonitor {

  private MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  private final InstanceType TEST_TYPE = InstanceType.PARTICIPANT;
  private final String TEST_CLUSTER = "test_cluster";

  private ObjectName buildObjectName(InstanceType type, String cluster,
      HelixConstants.ChangeType changeType) throws MalformedObjectNameException {
    return MBeanRegistrar.buildObjectName(MonitorDomainNames.HelixCallback.name(),
        HelixCallbackMonitor.MONITOR_TYPE, type.name(), HelixCallbackMonitor.MONITOR_KEY, cluster,
        HelixCallbackMonitor.MONITOR_CHANGE_TYPE, changeType.name());
  }

  private ObjectName buildObjectName(InstanceType type, String cluster,
      HelixConstants.ChangeType changeType, int num) throws MalformedObjectNameException {
    ObjectName objectName = buildObjectName(type, cluster, changeType);
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
    Set<HelixCallbackMonitor> monitors = new HashSet<>();
    for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
      monitors.add(new HelixCallbackMonitor(TEST_TYPE, TEST_CLUSTER, null, changeType).register());
      Assert.assertTrue(
          _beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, changeType)));
    }

    for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
      monitors.add(new HelixCallbackMonitor(TEST_TYPE, TEST_CLUSTER, null, changeType).register());
      Assert.assertTrue(
          _beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, changeType, 1)));
    }

    for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
      monitors.add(new HelixCallbackMonitor(TEST_TYPE, TEST_CLUSTER, null, changeType).register());
      Assert.assertTrue(
          _beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, changeType, 2)));
    }

    // Un-register all monitors
    for (HelixCallbackMonitor monitor : monitors) {
      monitor.unregister();
    }

    for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
      Assert.assertFalse(
          _beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, changeType)));
      Assert.assertFalse(
          _beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, changeType, 1)));
      Assert.assertFalse(
          _beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, changeType, 2)));
    }
  }

  @Test
  public void testCounter() throws JMException {
    HelixCallbackMonitor monitor = new HelixCallbackMonitor(TEST_TYPE, TEST_CLUSTER, null,
        HelixConstants.ChangeType.CURRENT_STATE);
    monitor.register();
    ObjectName name =
        buildObjectName(TEST_TYPE, TEST_CLUSTER, HelixConstants.ChangeType.CURRENT_STATE);

    monitor.increaseCallbackCounters(1000L);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "Counter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "LatencyCounter"), 1000L);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "LatencyGauge.Max"), 1000L);
    monitor.unregister();
  }
}
