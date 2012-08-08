package com.linkedin.helix.josql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ZNRecord;
public class TestZNRecordQueryProcessor
{
  private class TestTupleReader implements ZNRecordQueryProcessor.ZNRecordTupleReader
  {
    Map<String, List<ZNRecord>> map = new HashMap<String, List<ZNRecord>>();
    @Override
    public List<ZNRecord> get(String path) throws Exception
    {
      if(map.containsKey(path))
      {
        return map.get(path);
      }
      
      throw new Exception("Unable to read " + path);
    }
    
    public void reset()
    {
      map.clear();
    }
  }
  
  @Test
  public void testExplodeList() throws Exception
  {
    TestTupleReader tupleReader = new TestTupleReader();
    ZNRecord record = new ZNRecord("test");
    record.setListField("foo", Arrays.asList("val1", "val2", "val3"));
    tupleReader.map.put("test", Arrays.asList(record));
    String sql = "select * from explodeList(test, foo)";
    ZNRecordQueryProcessor queryProcessor = new ZNRecordQueryProcessor();
    List<ZNRecord> result = queryProcessor.execute(sql, tupleReader);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0).getSimpleField("listVal"), "val1");
    Assert.assertEquals(result.get(1).getSimpleField("listVal"), "val2");
    Assert.assertEquals(result.get(2).getSimpleField("listVal"), "val3");
  }
  
  @Test
  public void testExplodeMap() throws Exception
  {
    TestTupleReader tupleReader = new TestTupleReader();
    ZNRecord record = new ZNRecord("test");
    Map<String, String> map = new HashMap<String, String>();
    map.put("a", "100");
    map.put("b", "200");
    record.setMapField("foo", map);
    tupleReader.map.put("test", Arrays.asList(record));
    String sql = "select * from explodeMap(test, foo)";
    ZNRecordQueryProcessor queryProcessor = new ZNRecordQueryProcessor();
    List<ZNRecord> result = queryProcessor.execute(sql, tupleReader);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getSimpleField("a"), "100");
    Assert.assertEquals(result.get(0).getSimpleField("b"), "200");
  }
  
  @Test
  public void testJoin() throws Exception
  {
    TestTupleReader tupleReader = new TestTupleReader();
    List<ZNRecord> t1 = new ArrayList<ZNRecord>();
    for(int i = 1; i <= 3; i++)
    {
      ZNRecord record = new ZNRecord(""+i);
      record.setSimpleField("abc", "world" + i);
      record.setSimpleField("foo", "val" + i);
      t1.add(record);
    }
    tupleReader.map.put("T1", t1);

    List<ZNRecord> t2 = new ArrayList<ZNRecord>();
    for(int i = 1; i <= 5; i++)
    {
      ZNRecord record = new ZNRecord(""+i);
      record.setSimpleField("bar", "hello" + i);
      record.setSimpleField("foo", "val" + i);
      t2.add(record);
    }
    tupleReader.map.put("T2", t2);

    
    String sql = "select T1.abc, T2.bar from T1 join T2 using(T1.foo, T2.foo)";
    ZNRecordQueryProcessor queryProcessor = new ZNRecordQueryProcessor();
    List<ZNRecord> result = queryProcessor.execute(sql, tupleReader);
    System.out.println(result);
    for(int i = 0; i < 3; i++)
    {
      String a = result.get(i).getSimpleField("T1.abc").replace("world", "");
      String b = result.get(i).getSimpleField("T2.bar").replace("hello", "");
      Assert.assertEquals(a, b);
    }
    
    Assert.assertEquals(result.size(), 3);
  }
  
  @Test
  public void testMultipleJoin() throws Exception
  {
    TestTupleReader tupleReader = new TestTupleReader();
    List<ZNRecord> t1 = new ArrayList<ZNRecord>();
    for(int i = 1; i <= 8; i++)
    {
      ZNRecord record = new ZNRecord(""+i);
      record.setSimpleField("abc", "world" + i);
      record.setSimpleField("foo", "val" + i);
      t1.add(record);
    }
    tupleReader.map.put("T1", t1);

    List<ZNRecord> t2 = new ArrayList<ZNRecord>();
    for(int i = 1; i <= 5; i++)
    {
      ZNRecord record = new ZNRecord(""+i);
      record.setSimpleField("bar", "hello" + i);
      record.setSimpleField("foo", "val" + i);
      t2.add(record);
    }
    tupleReader.map.put("T2", t2);
    
    List<ZNRecord> t3 = new ArrayList<ZNRecord>();
    for(int i = 1; i <= 10; i++)
    {
      ZNRecord record = new ZNRecord(""+i);
      record.setSimpleField("xxx", "hey" + i);
      record.setSimpleField("foo", "val" + i);
      t3.add(record);
    }
    tupleReader.map.put("T3", t3);
    
    String sql = "select T1.abc, T2.bar, tt.xxx from T1 join T2 using(T2.foo, T2.foo) join T3 as tt using (T2.foo, tt.foo)";
    ZNRecordQueryProcessor queryProcessor = new ZNRecordQueryProcessor();
    List<ZNRecord> result = queryProcessor.execute(sql, tupleReader);
    System.out.println(result);
    
    for(int i = 0; i < 5; i++)
    {
      String a = result.get(i).getSimpleField("T1.abc").replace("world", "");
      String b = result.get(i).getSimpleField("T2.bar").replace("hello", "");
      String c = result.get(i).getSimpleField("tt.xxx").replace("hey", "");
      Assert.assertEquals(a, b);
    }
    
    Assert.assertEquals(result.size(), 5);

  }
  
  @Test
  public void testLargeJoin() throws Exception
  {
    TestTupleReader tupleReader = new TestTupleReader();
    List<ZNRecord> t1 = new ArrayList<ZNRecord>();
    for(int i = 0; i < 3000; i++)
    {
      ZNRecord record = new ZNRecord(""+i);
      record.setSimpleField("abc", "hello" + i);
      record.setSimpleField("foo", "val" + i);
      t1.add(record);
    }
    tupleReader.map.put("T1", t1);
    
    List<ZNRecord> t2 = new ArrayList<ZNRecord>();
    for(int i = 0; i < 5000; i++)
    {
      ZNRecord record = new ZNRecord(""+i);
      record.setSimpleField("bar", "world" + i);
      record.setSimpleField("foo", "val" + i);
      t2.add(record);
    }
    tupleReader.map.put("T2", t2);
    
    long start = System.currentTimeMillis();
    String sql = "select T1.abc, T2.bar from T1 join T2 using (T1.foo, T2.foo)";
    
    ZNRecordQueryProcessor queryProcessor = new ZNRecordQueryProcessor();
    List<ZNRecord> result = queryProcessor.execute(sql, tupleReader);
    System.out.println("XXX: " + (System.currentTimeMillis() - start));
    //System.out.println(result);
    Assert.assertEquals(result.size(), 3000);
  }
  
  @Test
  public void testSimpleGroupBy() throws Exception
  {
    TestTupleReader tupleReader = new TestTupleReader();
    List<ZNRecord> t = new ArrayList<ZNRecord>();
    tupleReader.map.put("T", t);
    for(int i = 0; i < 5000; i++)
    {
      ZNRecord record = new ZNRecord(""+i);
      record.setSimpleField("k", (i < 2500 ? "foo" : "bar"));
      record.setSimpleField("v", ""+i);
      t.add(record);    
    }
    String sql = "select k, max(to_number(v)) as maxv from T group by k";
    ZNRecordQueryProcessor queryProcessor = new ZNRecordQueryProcessor();
    List<ZNRecord> result = queryProcessor.execute(sql, tupleReader);
    Assert.assertEquals(result.size(), 2);
    for(ZNRecord record : result)
    {
      if(record.getSimpleField("k").equals("foo"))
      {
        Assert.assertEquals(record.getSimpleField("maxv"), "2499");
      }
      else
      {
        Assert.assertEquals(record.getSimpleField("maxv"), "4999");
      }
    }
  }
  
  @Test
  public void testMultipleGroupBy() throws Exception
  {
    TestTupleReader tupleReader = new TestTupleReader();
    List<ZNRecord> t = new ArrayList<ZNRecord>();
    tupleReader.map.put("T", t);

  }
}