package com.linkedin.clustermanager.monitoring.adaptors;

import java.util.List;

import com.linkedin.clustermanager.monitoring.Sensor;

public class JmxSensorAdaptor
{
  final String _domain;
  final Sensor<?> _sensor;
  public JmxSensorAdaptor(Sensor<?> sensor, String domain, List<String> filters)
  {
    _domain = domain;
    _sensor = sensor;
  }
}
