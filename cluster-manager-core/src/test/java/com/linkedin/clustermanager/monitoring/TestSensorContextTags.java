package com.linkedin.clustermanager.monitoring;

import java.util.*;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSensorContextTags
{
  @Test(groups={ "unitTest" })
  public void TestContextTags()
  {
    Map<String, String> tagMap = new HashMap<String, String>();
    tagMap.put("instanceName", "localhost_2181");
    tagMap.put("clusterName", "ESPRESSO_STORAGE");
    tagMap.put("resourceGroup", "db_COMM");
    tagMap.put("partition", "db_COMM_22");
    tagMap.put("HTTP_OP", "POST");
    tagMap.put("tableName", "T800");
    SensorContextTags tags1 = new SensorContextTags(tagMap);
    
    // Test to and from string
    String str = tags1.toString();
    
    SensorContextTags tags2 = SensorContextTags.fromString(str);
    Assert.assertTrue(tags2.containsTags(tagMap));
    Assert.assertTrue(tags1.containsTags(tags2.getTags()));
    Assert.assertTrue(tags2.containsTags(tags1.getTags()));
    Assert.assertTrue(tags1.getTags().size() == tagMap.size());
    
    // Test "contains"
    tagMap.put("NewTag", "newValue");
    
    Assert.assertFalse(tags1.containsTags(tagMap));
    tagMap.remove("HTTP_OP");
    Assert.assertFalse(tags1.containsTags(tagMap));
    tagMap.remove("NewTag");
    Assert.assertTrue(tags1.containsTags(tagMap));
    
    // Test filter
    Map<String, String> filterMap = new HashMap<String, String>();
    filterMap.put("instanceName", "localhost_2181");
    filterMap.put("clusterName", "ESPRESSO_STORAGE");
    filterMap.put("resourceGroup", "*");
    SensorTagFilter filter = new SensorTagFilter(filterMap);
    Assert.assertTrue(filter.matchs(tags2));
    
    SensorContextTags filteredTags = filter.getFilteredTags(tags2);
    Assert.assertEquals(filteredTags.getTags().size(), filterMap.size());
    Assert.assertTrue(tags2.containsTags(filteredTags.getTags()));
    
    filterMap.put("key", "value");
    filter = new SensorTagFilter(filterMap);
    Assert.assertFalse(filter.matchs(tags2));
  }
}
