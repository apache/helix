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

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reporter which listens for new metrics and exposes them as namespaced MBeans.
 * <p>
 * The code is largely based on the {@code JmxReporter} class of the dropwizard metrics library
 * <pre>See
 * <a href="https://github.com/dropwizard/metrics/blob/master/metrics-core/src/main/java/io/
 * dropwizard/metrics/JmxReporter.java">JmxReporter</a>
 * </pre>
 * <p>
 * It is recommended to be only used within Helix Rest.
 */
public class JmxReporter implements Reporter, Closeable {
  /**
   * Returns a new {@link Builder} for {@link JmxReporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link JmxReporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder for {@link JmxReporter} instances. Defaults to using the default MBean server and
   * not filtering metrics.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private MBeanServer mBeanServer;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private Hashtable<String, String> keyProperties;
    private MetricFilter filter = MetricFilter.ALL;
    private String domain;
    private Map<String, TimeUnit> specificDurationUnits;
    private Map<String, TimeUnit> specificRateUnits;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.domain = REST_JMX_DOMAIN;
      this.specificDurationUnits = Collections.emptyMap();
      this.specificRateUnits = Collections.emptyMap();
    }

    /**
     * Register MBeans with the given {@link MBeanServer}.
     *
     * @param mBeanServer an {@link MBeanServer}
     * @return {@code this}
     */
    public Builder registerWith(MBeanServer mBeanServer) {
      this.mBeanServer = mBeanServer;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Specifies key properties in the object name.
     *
     * @param keyProperties A hash table containing one or more key
     * properties.  The key of each entry in the table is the key of a
     * key property in the object name.
     * @return {Code this}
     */
    public Builder withKeyProperties(Hashtable<String, String> keyProperties) {
      this.keyProperties = keyProperties;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public Builder inDomain(String domain) {
      this.domain = domain;
      return this;
    }

    /**
     * Use specific {@link TimeUnit}s for the duration of the metrics with these names.
     *
     * @param specificDurationUnits a map of metric names and specific {@link TimeUnit}s
     * @return {@code this}
     */
    public Builder specificDurationUnits(Map<String, TimeUnit> specificDurationUnits) {
      this.specificDurationUnits = Collections.unmodifiableMap(specificDurationUnits);
      return this;
    }

    /**
     * Use specific {@link TimeUnit}s for the rate of the metrics with these names.
     *
     * @param specificRateUnits a map of metric names and specific {@link TimeUnit}s
     * @return {@code this}
     */
    public Builder specificRateUnits(Map<String, TimeUnit> specificRateUnits) {
      this.specificRateUnits = Collections.unmodifiableMap(specificRateUnits);
      return this;
    }

    /**
     * Builds a {@link JmxReporter} with the given properties.
     *
     * @return a {@link JmxReporter}
     */
    public JmxReporter build() {
      final MetricTimeUnits timeUnits =
          new MetricTimeUnits(rateUnit, durationUnit, specificRateUnits, specificDurationUnits);
      if (mBeanServer == null) {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
      }
      if (keyProperties == null) {
        keyProperties = new Hashtable<>();
      }
      return new JmxReporter(mBeanServer, domain, registry, filter, timeUnits, keyProperties);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(JmxReporter.class);

  private static final String REST_JMX_DOMAIN = "org.apache.helix.rest";
  private static final String REST_JMX_NAME_KEY = "name";
  private static final String REST_JMX_TYPE_KEY = "type";

  @SuppressWarnings("UnusedDeclaration")
  public interface MetricMBean {
    ObjectName objectName();
  }

  private abstract static class AbstractBean implements MetricMBean {
    private final ObjectName objectName;

    AbstractBean(ObjectName objectName) {
      this.objectName = objectName;
    }

    @Override
    public ObjectName objectName() {
      return objectName;
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxGaugeMBean extends MetricMBean {
    Object getValue();

    Number getNumber();
  }

  private static class JmxGauge extends AbstractBean implements JmxGaugeMBean {
    private final Gauge<?> metric;

    private JmxGauge(Gauge<?> metric, ObjectName objectName) {
      super(objectName);
      this.metric = metric;
    }

    @Override
    public Object getValue() {
      return metric.getValue();
    }

    @Override
    public Number getNumber() {
      Object value = metric.getValue();
      return value instanceof Number ? (Number) value : 0;
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxCounterMBean extends MetricMBean {
    long getCount();
  }

  private static class JmxCounter extends AbstractBean implements JmxCounterMBean {
    private final Counter metric;

    private JmxCounter(Counter metric, ObjectName objectName) {
      super(objectName);
      this.metric = metric;
    }

    @Override
    public long getCount() {
      return metric.getCount();
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxHistogramMBean extends MetricMBean {
    long getCount();

    long getMin();

    long getMax();

    double getMean();

    double getStdDev();

    double get50thPercentile();

    double get75thPercentile();

    double get95thPercentile();

    double get98thPercentile();

    double get99thPercentile();

    double get999thPercentile();

    long[] values();

    long getSnapshotSize();
  }

  private static class JmxHistogram implements JmxHistogramMBean {
    private final ObjectName objectName;
    private final Histogram metric;

    private JmxHistogram(Histogram metric, ObjectName objectName) {
      this.metric = metric;
      this.objectName = objectName;
    }

    @Override
    public ObjectName objectName() {
      return objectName;
    }

    @Override
    public double get50thPercentile() {
      return metric.getSnapshot().getMedian();
    }

    @Override
    public long getCount() {
      return metric.getCount();
    }

    @Override
    public long getMin() {
      return metric.getSnapshot().getMin();
    }

    @Override
    public long getMax() {
      return metric.getSnapshot().getMax();
    }

    @Override
    public double getMean() {
      return metric.getSnapshot().getMean();
    }

    @Override
    public double getStdDev() {
      return metric.getSnapshot().getStdDev();
    }

    @Override
    public double get75thPercentile() {
      return metric.getSnapshot().get75thPercentile();
    }

    @Override
    public double get95thPercentile() {
      return metric.getSnapshot().get95thPercentile();
    }

    @Override
    public double get98thPercentile() {
      return metric.getSnapshot().get98thPercentile();
    }

    @Override
    public double get99thPercentile() {
      return metric.getSnapshot().get99thPercentile();
    }

    @Override
    public double get999thPercentile() {
      return metric.getSnapshot().get999thPercentile();
    }

    @Override
    public long[] values() {
      return metric.getSnapshot().getValues();
    }

    @Override
    public long getSnapshotSize() {
      return metric.getSnapshot().size();
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxMeterMBean extends MetricMBean {
    long getCount();

    double getMeanRate();

    double getOneMinuteRate();

    double getFiveMinuteRate();

    double getFifteenMinuteRate();

    String getRateUnit();
  }

  private static class JmxMeter extends AbstractBean implements JmxMeterMBean {
    private final Metered metric;
    private final double rateFactor;
    private final String rateUnit;

    private JmxMeter(Metered metric, ObjectName objectName, TimeUnit rateUnit) {
      super(objectName);
      this.metric = metric;
      this.rateFactor = rateUnit.toSeconds(1);
      this.rateUnit = ("events/" + calculateRateUnit(rateUnit)).intern();
    }

    @Override
    public long getCount() {
      return metric.getCount();
    }

    @Override
    public double getMeanRate() {
      return metric.getMeanRate() * rateFactor;
    }

    @Override
    public double getOneMinuteRate() {
      return metric.getOneMinuteRate() * rateFactor;
    }

    @Override
    public double getFiveMinuteRate() {
      return metric.getFiveMinuteRate() * rateFactor;
    }

    @Override
    public double getFifteenMinuteRate() {
      return metric.getFifteenMinuteRate() * rateFactor;
    }

    @Override
    public String getRateUnit() {
      return rateUnit;
    }

    private String calculateRateUnit(TimeUnit unit) {
      final String s = unit.toString().toLowerCase(Locale.US);
      return s.substring(0, s.length() - 1);
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxTimerMBean extends JmxMeterMBean {
    double getMin();

    double getMax();

    double getMean();

    double getStdDev();

    double get50thPercentile();

    double get75thPercentile();

    double get95thPercentile();

    double get98thPercentile();

    double get99thPercentile();

    double get999thPercentile();

    long[] values();

    String getDurationUnit();
  }

  static class JmxTimer extends JmxMeter implements JmxTimerMBean {
    private final Timer metric;
    private final double durationFactor;
    private final String durationUnit;

    private JmxTimer(Timer metric, ObjectName objectName, TimeUnit rateUnit,
        TimeUnit durationUnit) {
      super(metric, objectName, rateUnit);
      this.metric = metric;
      this.durationFactor = 1.0 / durationUnit.toNanos(1);
      this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
    }

    @Override
    public double get50thPercentile() {
      return metric.getSnapshot().getMedian() * durationFactor;
    }

    @Override
    public double getMin() {
      return metric.getSnapshot().getMin() * durationFactor;
    }

    @Override
    public double getMax() {
      return metric.getSnapshot().getMax() * durationFactor;
    }

    @Override
    public double getMean() {
      return metric.getSnapshot().getMean() * durationFactor;
    }

    @Override
    public double getStdDev() {
      return metric.getSnapshot().getStdDev() * durationFactor;
    }

    @Override
    public double get75thPercentile() {
      return metric.getSnapshot().get75thPercentile() * durationFactor;
    }

    @Override
    public double get95thPercentile() {
      return metric.getSnapshot().get95thPercentile() * durationFactor;
    }

    @Override
    public double get98thPercentile() {
      return metric.getSnapshot().get98thPercentile() * durationFactor;
    }

    @Override
    public double get99thPercentile() {
      return metric.getSnapshot().get99thPercentile() * durationFactor;
    }

    @Override
    public double get999thPercentile() {
      return metric.getSnapshot().get999thPercentile() * durationFactor;
    }

    @Override
    public long[] values() {
      return metric.getSnapshot().getValues();
    }

    @Override
    public String getDurationUnit() {
      return durationUnit;
    }
  }

  private static class JmxListener implements MetricRegistryListener {
    private final String domainName;
    private final Hashtable<String, String> keyProperties;
    private final MBeanServer mBeanServer;
    private final MetricFilter filter;
    private final MetricTimeUnits timeUnits;
    private final Map<ObjectName, ObjectName> registered;

    private JmxListener(MBeanServer mBeanServer, String domainName, MetricFilter filter,
        MetricTimeUnits timeUnits, Hashtable<String, String> keyProperties) {
      this.mBeanServer = mBeanServer;
      this.domainName = domainName;
      this.filter = filter;
      this.timeUnits = timeUnits;
      this.registered = new ConcurrentHashMap<>();
      this.keyProperties = keyProperties;
    }

    private void registerMBean(Object mBean, ObjectName objectName) throws JMException {
      ObjectInstance objectInstance = mBeanServer.registerMBean(mBean, objectName);
      if (objectInstance != null) {
        // the websphere mbeanserver rewrites the objectname to include
        // cell, node & server info
        // make sure we capture the new objectName for unregistration
        registered.put(objectName, objectInstance.getObjectName());
      } else {
        registered.put(objectName, objectName);
      }
    }

    private void unregisterMBean(ObjectName originalObjectName)
        throws InstanceNotFoundException, MBeanRegistrationException {
      ObjectName storedObjectName = registered.remove(originalObjectName);
      if (storedObjectName != null) {
        mBeanServer.unregisterMBean(storedObjectName);
      } else {
        mBeanServer.unregisterMBean(originalObjectName);
      }
    }

    @Override
    public void onGaugeAdded(String name, Gauge<?> gauge) {
      try {
        if (filter.matches(name, gauge)) {
          final ObjectName objectName = createName("gauges", name, keyProperties);
          registerMBean(new JmxGauge(gauge, objectName), objectName);
        }
      } catch (InstanceAlreadyExistsException e) {
        LOG.debug("Unable to register gauge", e);
      } catch (JMException e) {
        LOG.warn("Unable to register gauge", e);
      }
    }

    @Override
    public void onGaugeRemoved(String name) {
      try {
        final ObjectName objectName = createName("gauges", name, keyProperties);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        LOG.debug("Unable to unregister gauge", e);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Unable to unregister gauge", e);
      }
    }

    @Override
    public void onCounterAdded(String name, Counter counter) {
      try {
        if (filter.matches(name, counter)) {
          final ObjectName objectName = createName("counters", name, keyProperties);
          registerMBean(new JmxCounter(counter, objectName), objectName);
        }
      } catch (InstanceAlreadyExistsException e) {
        LOG.debug("Unable to register counter", e);
      } catch (JMException e) {
        LOG.warn("Unable to register counter", e);
      }
    }

    @Override
    public void onCounterRemoved(String name) {
      try {
        final ObjectName objectName = createName("counters", name, keyProperties);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        LOG.debug("Unable to unregister counter", e);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Unable to unregister counter", e);
      }
    }

    @Override
    public void onHistogramAdded(String name, Histogram histogram) {
      try {
        if (filter.matches(name, histogram)) {
          final ObjectName objectName = createName("histograms", name, keyProperties);
          registerMBean(new JmxHistogram(histogram, objectName), objectName);
        }
      } catch (InstanceAlreadyExistsException e) {
        LOG.debug("Unable to register histogram", e);
      } catch (JMException e) {
        LOG.warn("Unable to register histogram", e);
      }
    }

    @Override
    public void onHistogramRemoved(String name) {
      try {
        final ObjectName objectName = createName("histograms", name, keyProperties);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        LOG.debug("Unable to unregister histogram", e);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Unable to unregister histogram", e);
      }
    }

    @Override
    public void onMeterAdded(String name, Meter meter) {
      try {
        if (filter.matches(name, meter)) {
          final ObjectName objectName = createName("meters", name, keyProperties);
          registerMBean(new JmxMeter(meter, objectName, timeUnits.rateFor(name)), objectName);
        }
      } catch (InstanceAlreadyExistsException e) {
        LOG.debug("Unable to register meter", e);
      } catch (JMException e) {
        LOG.warn("Unable to register meter", e);
      }
    }

    @Override
    public void onMeterRemoved(String name) {
      try {
        final ObjectName objectName = createName("meters", name, keyProperties);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        LOG.debug("Unable to unregister meter", e);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Unable to unregister meter", e);
      }
    }

    @Override
    public void onTimerAdded(String name, Timer timer) {
      try {
        if (filter.matches(name, timer)) {
          final ObjectName objectName = createName("timers", name, keyProperties);
          registerMBean(
              new JmxTimer(timer, objectName, timeUnits.rateFor(name), timeUnits.durationFor(name)),
              objectName);
        }
      } catch (InstanceAlreadyExistsException e) {
        LOG.debug("Unable to register timer", e);
      } catch (JMException e) {
        LOG.warn("Unable to register timer", e);
      }
    }

    @Override
    public void onTimerRemoved(String name) {
      try {
        final ObjectName objectName = createName("timers", name, keyProperties);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        LOG.debug("Unable to unregister timer", e);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Unable to unregister timer", e);
      }
    }

    /*
     * Create an object name that must have "type" and "name" keys.
     * And flexibly, extra keys can be added within keyProperties.
     * Eg. object name: "org.apache.helix.rest:name=requests-count,type=counters,
     * namespace=myNamespace"
     */
    private ObjectName createName(String type, String name,
        Hashtable<String, String> keyProperties) {
      keyProperties.put(REST_JMX_TYPE_KEY, type);
      keyProperties.put(REST_JMX_NAME_KEY, name);

      try {
        ObjectName objectName = new ObjectName(this.domainName, keyProperties);
        /*
         * The only way we can find out if we need to quote the properties is by
         * checking an ObjectName that we've constructed.
         */
        String domain = this.domainName;
        if (objectName.isDomainPattern()) {
          domain = ObjectName.quote(this.domainName);
        }
        if (objectName.isPropertyValuePattern(REST_JMX_NAME_KEY)) {
          keyProperties.put(REST_JMX_NAME_KEY, ObjectName.quote(name));
        }
        if (objectName.isPropertyValuePattern(REST_JMX_TYPE_KEY)) {
          keyProperties.put(REST_JMX_TYPE_KEY, ObjectName.quote(type));
        }

        return new ObjectName(domain, keyProperties);
      } catch (MalformedObjectNameException e) {
        /*
         * There is an implementation error on our side if this occurs. Either the domain was
         * modified and no longer conforms to the JMX domain rules or the table wasn't properly
         * generated.
         */
        LOG.warn("Implementation error. The domain or table does not conform to JMX rules.", e);
        return null;
      }
    }

    void unregisterAll() {
      for (ObjectName name : registered.keySet()) {
        try {
          unregisterMBean(name);
        } catch (InstanceNotFoundException e) {
          LOG.debug("Unable to unregister metric", e);
        } catch (MBeanRegistrationException e) {
          LOG.warn("Unable to unregister metric", e);
        }
      }
      registered.clear();
    }
  }

  private static class MetricTimeUnits {
    private final TimeUnit defaultRate;
    private final TimeUnit defaultDuration;
    private final Map<String, TimeUnit> rateOverrides;
    private final Map<String, TimeUnit> durationOverrides;

    MetricTimeUnits(TimeUnit defaultRate, TimeUnit defaultDuration,
        Map<String, TimeUnit> rateOverrides, Map<String, TimeUnit> durationOverrides) {
      this.defaultRate = defaultRate;
      this.defaultDuration = defaultDuration;
      this.rateOverrides = rateOverrides;
      this.durationOverrides = durationOverrides;
    }

    public TimeUnit durationFor(String name) {
      return durationOverrides.getOrDefault(name, defaultDuration);
    }

    public TimeUnit rateFor(String name) {
      return rateOverrides.getOrDefault(name, defaultRate);
    }
  }

  private final MetricRegistry registry;
  private final JmxListener listener;

  private JmxReporter(MBeanServer mBeanServer, String domain, MetricRegistry registry,
      MetricFilter filter, MetricTimeUnits timeUnits, Hashtable<String, String> keyProperties) {
    this.registry = registry;
    this.listener = new JmxListener(mBeanServer, domain, filter, timeUnits, keyProperties);
  }

  /**
   * Starts the reporter.
   */
  public void start() {
    registry.addListener(listener);
  }

  /**
   * Stops the reporter.
   */
  public void stop() {
    registry.removeListener(listener);
    listener.unregisterAll();
  }

  /**
   * Stops the reporter.
   */
  @Override
  public void close() {
    stop();
  }
}
