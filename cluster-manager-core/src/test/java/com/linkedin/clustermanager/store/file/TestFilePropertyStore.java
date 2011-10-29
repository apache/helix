package com.linkedin.clustermanager.store.file;

import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;

public class TestFilePropertyStore
{
  private static Logger logger = Logger.getLogger(TestFilePropertyStore.class);
  private static final String rootNamespace = "/tmp/TestFilePropertyStore";
  
  public class TestPropertyChangeListener implements PropertyChangeListener<String>
  {
    public boolean _propertyChangeReceived = false;
    
    @Override
    public void onPropertyChange(String key)
    {
      logger.info("property changed at " + key);
      _propertyChangeReceived = true;
    }
    
  }
  
  public class TestUpdater implements DataUpdater<String>
  {

    @Override
    public String update(String currentData)
    {
      return "new " + currentData;
    }
    
  }
  
  @Test (groups = {"unitTest"})
  public void testFilePropertyStore() throws Exception
  {
    System.out.println("START TestFilePropertyStore at " + new Date(System.currentTimeMillis()));
    
    final int SLEEP_TIME = 2000;
    PropertyJsonSerializer<String> serializer = new PropertyJsonSerializer<String>(String.class);
    PropertyJsonComparator<String> comparator = new PropertyJsonComparator<String>(String.class);
    
    FilePropertyStore<String> store = new FilePropertyStore<String>(serializer, rootNamespace, 
        comparator);
    // store.removeRootNamespace();
    // store.createRootNamespace();
    store.start();
    
    // test set
    store.createPropertyNamespace("/child1");
    store.setProperty("/child1/grandchild1", "grandchild1\n");
    store.setProperty("/child1/grandchild2", "grandchild2\n");
    store.createPropertyNamespace("/child1/grandchild3");
    store.setProperty("/child1/grandchild3/grandgrandchild1", "grandgrandchild1\n");

    // test get-names
    List<String> names = store.getPropertyNames("/child1");
    AssertJUnit.assertEquals(names.size(), 3);
    AssertJUnit.assertTrue(names.contains("/child1/grandchild1"));
    AssertJUnit.assertTrue(names.contains("/child1/grandchild2"));
    AssertJUnit.assertTrue(names.contains("/child1/grandchild3/grandgrandchild1"));
    
    // test get
    String value = store.getProperty("nonExist");
    AssertJUnit.assertEquals(value, null);
    value = store.getProperty("/child1/grandchild2");
    AssertJUnit.assertEquals(value, "grandchild2\n");
    Thread.sleep(SLEEP_TIME);
    
    // test subscribe
    TestPropertyChangeListener listener1 = new TestPropertyChangeListener();
    TestPropertyChangeListener listener2 = new TestPropertyChangeListener();
    
    store.subscribeForPropertyChange("/child1", listener1);
    store.subscribeForPropertyChange("/child1", listener1);
    store.subscribeForPropertyChange("/child1", listener2);
    
    store.setProperty("/child1/grandchild2", "grandchild2-new\n");
    Thread.sleep(SLEEP_TIME);
    AssertJUnit.assertEquals(listener1._propertyChangeReceived, true);
    AssertJUnit.assertEquals(listener2._propertyChangeReceived, true);
    
    listener1._propertyChangeReceived = false;
    listener2._propertyChangeReceived = false;
    
    // test unsubscribe
    store.unsubscribeForPropertyChange("/child1", listener1);
    store.setProperty("/child1/grandchild3/grandgrandchild1", "grandgrandchild1-new\n");
    Thread.sleep(SLEEP_TIME);
    
    AssertJUnit.assertEquals(listener1._propertyChangeReceived, false);
    AssertJUnit.assertEquals(listener2._propertyChangeReceived, true);
    
    listener2._propertyChangeReceived = false;

    // test update property
    store.updatePropertyUntilSucceed("child1/grandchild2", new TestUpdater());
    value = store.getProperty("child1/grandchild2");
    AssertJUnit.assertEquals(value, "new grandchild2-new\n");
    
    // test remove
    store.removeProperty("/child1/grandchild2");
    value = store.getProperty("/child1/grandchild2");
    AssertJUnit.assertEquals(value, null);
    Thread.sleep(SLEEP_TIME);
    AssertJUnit.assertEquals(listener2._propertyChangeReceived, true);
    listener2._propertyChangeReceived = false;
    
    // test compare and set
    boolean success = store.compareAndSet("/child1/grandchild1", "grandchild1-old\n", 
                                          "grandchild1-new\n", comparator);
    AssertJUnit.assertEquals(success, false);
    
    success = store.compareAndSet("/child1/grandchild1", "grandchild1\n", 
                                  "grandchild1-new\n", comparator);
    AssertJUnit.assertEquals(success, true);
    
    store.stop();
    
    // test stop
    listener2._propertyChangeReceived = false;
    store.setProperty("/child1/grandchild3/grandgrandchild1", "grandgrandchild1-new-new\n");
    Thread.sleep(SLEEP_TIME);
    AssertJUnit.assertEquals(listener2._propertyChangeReceived, false);
    
    store.unsubscribeForPropertyChange("/child1", listener2);
    // store.stop();
    System.out.println("END TestFilePropertyStore at " + new Date(System.currentTimeMillis()));
  }
}
