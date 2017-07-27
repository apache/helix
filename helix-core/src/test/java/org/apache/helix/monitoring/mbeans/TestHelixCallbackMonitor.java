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

  private ObjectName buildObjectName(InstanceType type, String cluster)
      throws MalformedObjectNameException {
    return buildObjectName(type, cluster, 0);
  }

  private ObjectName buildObjectName(InstanceType type, String cluster, int num)
      throws MalformedObjectNameException {
    return MBeanRegistrar.buildObjectName(num, MonitorDomainNames.HelixCallback.name(),
        HelixCallbackMonitor.INSTANCE_TYPE, type.name(), HelixCallbackMonitor.CLUSTER,
        cluster);
  }

  @Test public void testMBeanRegisteration() throws JMException {
    Set<HelixCallbackMonitor> monitors = new HashSet<>();
    monitors.add(new HelixCallbackMonitor(TEST_TYPE, TEST_CLUSTER));
    Assert.assertTrue(_beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER)));

    monitors.add(new HelixCallbackMonitor(TEST_TYPE, TEST_CLUSTER));
    Assert.assertTrue(_beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, 1)));

    HelixCallbackMonitor testMonitor =
        new HelixCallbackMonitor(TEST_TYPE, TEST_CLUSTER);
    monitors.add(testMonitor);
    Assert.assertTrue(_beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, 2)));

    // Un-register all monitors
    for (HelixCallbackMonitor monitor : monitors) {
      monitor.unregister();
    }

    Assert.assertFalse(_beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER)));
    Assert.assertFalse(_beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, 1)));
    Assert.assertFalse(_beanServer.isRegistered(buildObjectName(TEST_TYPE, TEST_CLUSTER, 2)));
  }

  @Test public void testCounter() throws JMException {
    HelixCallbackMonitor monitor = new HelixCallbackMonitor(TEST_TYPE, TEST_CLUSTER);
    ObjectName name = buildObjectName(TEST_TYPE, TEST_CLUSTER);

    monitor.increaseCallbackCounters(HelixConstants.ChangeType.CURRENT_STATE, 1000L);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "CallbackCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "CallbackLatencyCounter"), 1000L);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "CurrentStateCallbackCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(name, "CurrentStateCallbackLatencyCounter"),
        1000L);

    monitor.unregister();
  }
}
