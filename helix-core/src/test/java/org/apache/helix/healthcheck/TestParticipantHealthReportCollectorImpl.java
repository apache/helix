package org.apache.helix.healthcheck;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.Mocks.MockManager;
import org.apache.helix.healthcheck.ParticipantHealthReportCollectorImpl;
import org.apache.helix.healthcheck.ParticipantHealthReportTask;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestParticipantHealthReportCollectorImpl {

  protected ParticipantHealthReportCollectorImpl _providerImpl;
  protected ParticipantHealthReportTask _providerTask;
  protected HelixManager _manager;
  protected MockHealthReportProvider _mockProvider;

  public static class MockHealthReportProvider extends HealthReportProvider {

    @Override
    public Map<String, String> getRecentHealthReport() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void resetStats() {
      // TODO Auto-generated method stub

    }

  }

  @BeforeMethod()
  public void setup() {
    _providerImpl = new ParticipantHealthReportCollectorImpl(new MockManager(), "instance_123");
    _providerTask = new ParticipantHealthReportTask(_providerImpl);
    _mockProvider = new MockHealthReportProvider();
  }

  @Test()
  public void testStart() throws Exception {
    _providerTask.start();
    _providerTask.start();
  }

  @Test()
  public void testStop() throws Exception {
    _providerTask.stop();
    _providerTask.stop();
  }

  @Test()
  public void testAddProvider() throws Exception {
    _providerImpl.removeHealthReportProvider(_mockProvider);
    _providerImpl.addHealthReportProvider(_mockProvider);
    _providerImpl.addHealthReportProvider(_mockProvider);
  }

  @Test()
  public void testRemoveProvider() throws Exception {
    _providerImpl.addHealthReportProvider(_mockProvider);
    _providerImpl.removeHealthReportProvider(_mockProvider);
    _providerImpl.removeHealthReportProvider(_mockProvider);
  }
}
