package com.linkedin.helix.manager.file;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.InstanceType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.manager.MockListener;
import com.linkedin.helix.manager.file.CallbackHandlerForFile;
import com.linkedin.helix.store.PropertyJsonComparator;
import com.linkedin.helix.store.PropertyJsonSerializer;
import com.linkedin.helix.store.PropertyStoreException;
import com.linkedin.helix.store.file.FilePropertyStore;

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
    CallbackHandlerForFile handler =
        new CallbackHandlerForFile(manager,
                                   rootNamespace,
                                   listener,
                                   new EventType[] { EventType.NodeChildrenChanged,
                                       EventType.NodeDeleted, EventType.NodeCreated },
                                   ChangeType.CONFIG);
    AssertJUnit.assertEquals(listener, handler.getListener());
    AssertJUnit.assertEquals(rootNamespace, handler.getPath());
    AssertJUnit.assertTrue(listener.isConfigChangeListenerInvoked);

    handler =
        new CallbackHandlerForFile(manager,
                                   rootNamespace,
                                   listener,
                                   new EventType[] { EventType.NodeChildrenChanged,
                                       EventType.NodeDeleted, EventType.NodeCreated },
                                   ChangeType.EXTERNAL_VIEW);
    AssertJUnit.assertTrue(listener.isExternalViewChangeListenerInvoked);

    EventType[] eventTypes = new EventType[] { EventType.NodeChildrenChanged,
        EventType.NodeDeleted, EventType.NodeCreated };
    handler =
        new CallbackHandlerForFile(manager,
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
