package org.apache.helix.manager.file;

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

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.manager.MockListener;
import org.apache.helix.manager.file.FileCallbackHandler;
import org.apache.helix.store.PropertyJsonComparator;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.store.PropertyStoreException;
import org.apache.helix.store.file.FilePropertyStore;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestFileCallbackHandler
{
  @Test(groups = { "unitTest" })
  public void testFileCallbackHandler()
  {
    final String clusterName = "TestFileCallbackHandler";
    final String rootNamespace = "/tmp/" + clusterName;
    final String instanceName = "controller_0";
    MockListener listener = new MockListener();

    PropertyJsonSerializer<ZNRecord> serializer =
        new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    PropertyJsonComparator<ZNRecord> comparator =
        new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    FilePropertyStore<ZNRecord> store =
        new FilePropertyStore<ZNRecord>(serializer, rootNamespace, comparator);
    try
    {
      store.removeRootNamespace();
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    listener.reset();
    MockFileHelixManager manager =
        new MockFileHelixManager(clusterName, instanceName, InstanceType.CONTROLLER, store);
    FileCallbackHandler handler =
        new FileCallbackHandler(manager,
                                   rootNamespace,
                                   listener,
                                   new EventType[] { EventType.NodeChildrenChanged,
                                       EventType.NodeDeleted, EventType.NodeCreated },
                                   ChangeType.CONFIG);
    AssertJUnit.assertEquals(listener, handler.getListener());
    AssertJUnit.assertEquals(rootNamespace, handler.getPath());
    AssertJUnit.assertTrue(listener.isConfigChangeListenerInvoked);

    handler =
        new FileCallbackHandler(manager,
                                   rootNamespace,
                                   listener,
                                   new EventType[] { EventType.NodeChildrenChanged,
                                       EventType.NodeDeleted, EventType.NodeCreated },
                                   ChangeType.EXTERNAL_VIEW);
    AssertJUnit.assertTrue(listener.isExternalViewChangeListenerInvoked);

    EventType[] eventTypes = new EventType[] { EventType.NodeChildrenChanged,
        EventType.NodeDeleted, EventType.NodeCreated };
    handler =
        new FileCallbackHandler(manager,
                                   rootNamespace,
                                   listener,
                                   eventTypes,
                                   ChangeType.CONTROLLER);
    AssertJUnit.assertEquals(handler.getEventTypes(), eventTypes);
    AssertJUnit.assertTrue(listener.isControllerChangeListenerInvoked);
    
    listener.reset();
    handler.reset();
    AssertJUnit.assertTrue(listener.isControllerChangeListenerInvoked);

    listener.reset();
    handler.onPropertyChange(rootNamespace);
    AssertJUnit.assertTrue(listener.isControllerChangeListenerInvoked);

    store.stop();
  }
}
