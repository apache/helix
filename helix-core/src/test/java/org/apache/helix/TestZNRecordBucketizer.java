package org.apache.helix;

import org.apache.helix.ZNRecordBucketizer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZNRecordBucketizer
{
  @Test
  public void testZNRecordBucketizer()
  {
    final int bucketSize = 3;
    ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(bucketSize);
    String[] partitionNames =
        { "TestDB_0", "TestDB_1", "TestDB_2", "TestDB_3", "TestDB_4" };
    for (int i = 0; i < partitionNames.length; i++)
    {
      String partitionName = partitionNames[i];
      String bucketName = bucketizer.getBucketName(partitionName);
      int startBucketNb = i / bucketSize * bucketSize;
      int endBucketNb = startBucketNb + bucketSize - 1;
      String expectBucketName = "TestDB_p" + startBucketNb + "-p" + endBucketNb;
      System.out.println("Expect: " + expectBucketName + ", actual: " + bucketName);
      Assert.assertEquals(expectBucketName, bucketName);
      
    }
    
//    ZNRecord record = new ZNRecord("TestDB");
//    record.setSimpleField("key0", "value0");
//    record.setSimpleField("key1", "value1");
//    record.setListField("TestDB_0", Arrays.asList("localhost_00", "localhost_01"));
//    record.setListField("TestDB_1", Arrays.asList("localhost_10", "localhost_11"));
//    record.setListField("TestDB_2", Arrays.asList("localhost_20", "localhost_21"));
//    record.setListField("TestDB_3", Arrays.asList("localhost_30", "localhost_31"));
//    record.setListField("TestDB_4", Arrays.asList("localhost_40", "localhost_41"));
//    
//    System.out.println(bucketizer.bucketize(record));
    
  }
}
