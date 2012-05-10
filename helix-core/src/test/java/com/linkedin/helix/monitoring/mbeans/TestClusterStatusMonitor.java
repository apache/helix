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
package com.linkedin.helix.monitoring.mbeans;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.testng.annotations.Test;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.Mocks;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;
import com.linkedin.helix.monitoring.mbeans.ClusterStatusMonitor;
import com.linkedin.helix.tools.IdealStateCalculatorForStorageNode;


public class TestClusterStatusMonitor
{
  List<String> _instances;
  List<ZNRecord> _liveInstances;
  String _db = "DB";
  String _db2 = "TestDB";
  int _replicas = 3;
  int _partitions = 50;
  ZNRecord _externalView, _externalView2;

  class MockDataAccessor extends Mocks.MockAccessor
  {
    public MockDataAccessor()
    {
      _instances = new ArrayList<String>();
      for(int i = 0;i < 5; i++)
      {
        String instance = "localhost_"+(12918+i);
        _instances.add(instance);
      }
      ZNRecord externalView = IdealStateCalculatorForStorageNode.calculateIdealState(
          _instances, _partitions, _replicas, _db, "MASTER", "SLAVE");

      ZNRecord externalView2 = IdealStateCalculatorForStorageNode.calculateIdealState(
          _instances, 80, 2, _db2, "MASTER", "SLAVE");

    }
    public ZNRecord getProperty(PropertyType type, String resource)
    {
      if(type == PropertyType.IDEALSTATES || type == PropertyType.EXTERNALVIEW)
      {
        if(resource.equals(_db))
        {
          return _externalView;
        }
        else if(resource.equals(_db2))
        {
          return _externalView2;
        }
      }
      return null;
    }
  }
  class MockHelixManager extends Mocks.MockManager
  {
    MockDataAccessor _accessor = new MockDataAccessor();

    @Override
		public DataAccessor getDataAccessor()
    {
      return _accessor;
    }

  }
  @Test()
  public void TestReportData()
  {
  	System.out.println("START TestClusterStatusMonitor at" + new Date(System.currentTimeMillis()));
    List<String> _instances;
    List<ZNRecord> _liveInstances = new ArrayList<ZNRecord>();
    String _db = "DB";
    int _replicas = 3;
    int _partitions = 50;

    _instances = new ArrayList<String>();
    for(int i = 0;i < 5; i++)
    {
      String instance = "localhost_"+(12918+i);
      _instances.add(instance);
      ZNRecord metaData = new ZNRecord(instance);
      metaData.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(),
          UUID.randomUUID().toString());
      _liveInstances.add(metaData);
    }
    ZNRecord externalView = IdealStateCalculatorForStorageNode.calculateIdealState(
        _instances, _partitions, _replicas, _db, "MASTER", "SLAVE");

    ZNRecord externalView2 = IdealStateCalculatorForStorageNode.calculateIdealState(
        _instances, 80, 2, "TestDB", "MASTER", "SLAVE");

    List<ZNRecord> externalViews = new ArrayList<ZNRecord>();
    externalViews.add(externalView);
    externalViews.add(externalView2);

    ClusterStatusMonitor monitor = new ClusterStatusMonitor("cluster1");
    MockHelixManager manager = new MockHelixManager();
    NotificationContext context = new NotificationContext(manager);
    System.out.println("END TestClusterStatusMonitor at" + new Date(System.currentTimeMillis()));
  }
}
