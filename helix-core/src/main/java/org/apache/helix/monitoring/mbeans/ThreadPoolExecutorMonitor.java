package org.apache.helix.monitoring.mbeans;

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

import java.util.concurrent.ThreadPoolExecutor;
import javax.management.JMException;
import javax.management.ObjectName;

public class ThreadPoolExecutorMonitor implements ThreadPoolExecutorMonitorMBean {
  public static final String TYPE = "Type";

  private ObjectName _objectName;
  private ThreadPoolExecutor _executor;
  private String _type;

  public ThreadPoolExecutorMonitor(String type, ThreadPoolExecutor executor)
      throws JMException {
    _type = type;
    _executor = executor;
    _objectName = MBeanRegistrar
        .register(this, MonitorDomainNames.HelixThreadPoolExecutor.name(), TYPE, type);
  }

  public void unregister() {
    MBeanRegistrar.unregister(_objectName);
  }

  @Override
  public String getSensorName() {
    return String
        .format("%s.%s", MonitorDomainNames.HelixThreadPoolExecutor.name(), _type);
  }

  @Override
  public int getThreadPoolCoreSizeGauge() {
    return _executor.getCorePoolSize();
  }

  @Override
  public int getThreadPoolMaxSizeGauge() {
    return _executor.getMaximumPoolSize();
  }

  @Override
  public int getQueueSizeGauge() {
    return _executor.getQueue().size();
  }

  @Override
  public int getNumOfActiveThreadsGauge() {
    return _executor.getActiveCount();
  }
}
