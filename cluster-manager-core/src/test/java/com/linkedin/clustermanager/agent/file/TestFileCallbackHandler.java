package com.linkedin.clustermanager.agent.file;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.CMConstants.ChangeType;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.store.file.FilePropertyStore;

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
    MockFileClusterManager manager =
        new MockFileClusterManager(clusterName, instanceName, InstanceType.CONTROLLER, store);
    CallbackHandlerForFile handler =
        new CallbackHandlerForFile(manager,
                                   rootNamespace,
                                   listener,
                                   new EventType[] { EventType.NodeChildrenChanged,
                                       EventType.NodeDeleted, EventType.NodeCreated },
                                   ChangeType.CONFIG);
    Assert.assertEquals(listener, handler.getListener());
    Assert.assertEquals(rootNamespace, handler.getPath());
    Assert.assertTrue(listener.isConfigChangeListenerInvoked);

    handler =
        new CallbackHandlerForFile(manager,
                                   rootNamespace,
                                   listener,
                                   new EventType[] { EventType.NodeChildrenChanged,
                                       EventType.NodeDeleted, EventType.NodeCreated },
                                   ChangeType.EXTERNAL_VIEW);
    Assert.assertTrue(listener.isExternalViewChangeListenerInvoked);

    EventType[] eventTypes = new EventType[] { EventType.NodeChildrenChanged,
        EventType.NodeDeleted, EventType.NodeCreated };
    handler =
        new CallbackHandlerForFile(manager,
                                   rootNamespace,
                                   listener,
                                   eventTypes,
                                   ChangeType.CONTROLLER);
    Assert.assertEquals(handler.getEventTypes(), eventTypes);
    Assert.assertTrue(listener.isControllerChangeListenerInvoked);
    
    listener.reset();
    handler.reset();
    Assert.assertTrue(listener.isControllerChangeListenerInvoked);

    listener.reset();
    handler.onPropertyChange(rootNamespace);
    Assert.assertTrue(listener.isControllerChangeListenerInvoked);

    store.stop();
  }
}
