package com.linkedin.clustermanager;

import java.util.List;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.file.FilePropertyStore;

public class TestFilePropertyStore
{
  
  public class MyPropertyChangeListener implements PropertyChangeListener<String>
  {
    public boolean _propertyChangeReceived = false;
    
    @Override
    public void onPropertyChange(String key)
    {
      // TODO Auto-generated method stub
      System.out.println("property changed at " + key);
      _propertyChangeReceived = true;
    }
    
  }
  
  /**
  public class MyComparator implements Comparator<String>
  {

    @Override
    public int compare(String o1, String o2)
    {
      if (o1 == null && o2 == null)
      {
        return 0;
      }
      else if (o1 == null && o2 != null)
      {
        return -1;
      }
      else if (o1 != null && o2 == null)
      {
        return 1;
      }
      else
      {
        return o1.compareTo(o2);
      }
    }
    
  }
  **/
  
  @Test
  public void testInvocation() throws Exception
  {
    // StringPropertySerializer serializer = new StringPropertySerializer();
    PropertyJsonSerializer<String> serializer = new PropertyJsonSerializer<String>(String.class);
    String rootNamespace = "/tmp/testFilePropertyStore";
    
    FilePropertyStore<String> store = new FilePropertyStore<String>(serializer, rootNamespace);
    store.removeRootNamespace();
    store.createRootNamespace();
    store.start();
    
    // test set
    store.createPropertyNamespace("testPath1");
    store.setProperty("testPath1/testPath2", "testValue2-I\n");
    store.setProperty("testPath1/testPath3", "testValue3-I\n");

    // test get-names
    List<String> names = store.getPropertyNames("testPath1");
    Assert.assertEquals(names.size(), 2);
    Assert.assertEquals(names.get(0), "testPath1/testPath2");
    Assert.assertEquals(names.get(1), "testPath1/testPath3");
    
    // test get
    String value = store.getProperty("nonExist");
    Assert.assertEquals(value, null);
    value = store.getProperty("testPath1/testPath2");
    Assert.assertEquals(value, "testValue2-I\n");
    Thread.sleep(1000);
    
    // test subscribe
    MyPropertyChangeListener listener = new MyPropertyChangeListener();
    MyPropertyChangeListener listener2 = new MyPropertyChangeListener();
    
    store.subscribeForPropertyChange("testPath1", listener);
    store.subscribeForPropertyChange("testPath1", listener);
    store.subscribeForPropertyChange("testPath1", listener2);
    
    store.setProperty("testPath1/testPath3", "testValue3-II\n");
    Thread.sleep(1000);
    Assert.assertEquals(listener._propertyChangeReceived, true);
    Assert.assertEquals(listener2._propertyChangeReceived, true);
    
    listener._propertyChangeReceived = false;
    listener2._propertyChangeReceived = false;
    
    // test unsubscribe
    store.unsubscribeForPropertyChange("testPath1", listener);
    store.setProperty("testPath1/testPath4", "testValue4-I\n");
    Thread.sleep(1000);
    
    Assert.assertEquals(listener._propertyChangeReceived, false);
    Assert.assertEquals(listener2._propertyChangeReceived, true);
    
    listener2._propertyChangeReceived = false;
    
    // test remove
    store.removeProperty("testPath1/testPath3");
    value = store.getProperty("testPath1/testPath3");
    Assert.assertEquals(value, null);
    Thread.sleep(1000);
    Assert.assertEquals(listener2._propertyChangeReceived, true);
    listener2._propertyChangeReceived = false;
    
    // test compare and set
    boolean success = store.compareAndSet("testPath1/testPath4", 
                                          "testValue4-II\n", 
                                          "testValue4-II\n", 
                                          new PropertyJsonComparator<String>(String.class));
    Assert.assertEquals(success, false);
    
    success = store.compareAndSet("testPath1/testPath4", 
                                  "testValue4-I\n", 
                                  "testValue4-II\n", 
                                  new PropertyJsonComparator<String>(String.class));
    Assert.assertEquals(success, true);
    
    store.unsubscribeForPropertyChange("testPath1", listener2);
    store.stop();
  }
}
