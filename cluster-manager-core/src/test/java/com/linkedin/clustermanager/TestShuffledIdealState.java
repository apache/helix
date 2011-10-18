package com.linkedin.clustermanager;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.tools.IdealCalculatorByConsistentHashing;
import com.linkedin.clustermanager.tools.IdealStateCalculatorByRush;
import com.linkedin.clustermanager.tools.IdealStateCalculatorByShuffling;

public class TestShuffledIdealState
{
  @Test (groups = {"unitTest"})
  public void testInvocation() throws Exception
  {

    int partitions = 6, replicas = 2;
    String dbName = "espressoDB1";
    List<String> instanceNames = new ArrayList<String>();
    instanceNames.add("localhost_1231");
    instanceNames.add("localhost_1232");
    instanceNames.add("localhost_1233");
    instanceNames.add("localhost_1234");

    ZNRecord result = IdealStateCalculatorByShuffling.calculateIdealState(
        instanceNames, partitions, replicas, dbName);
    
    ZNRecord result2 = IdealStateCalculatorByRush.calculateIdealState(instanceNames, 1, partitions, replicas, dbName);
    
    ZNRecord result3 = IdealCalculatorByConsistentHashing.calculateIdealState(instanceNames, partitions, replicas, dbName, new IdealCalculatorByConsistentHashing.FnvHash());
    IdealCalculatorByConsistentHashing.printIdealStateStats(result3, "MASTER");
    IdealCalculatorByConsistentHashing.printIdealStateStats(result3, "SLAVE");
    IdealCalculatorByConsistentHashing.printIdealStateStats(result3, "");
    IdealCalculatorByConsistentHashing.printNodeOfflineOverhead(result3);
    
    // System.out.println(result);
    ObjectMapper mapper = new ObjectMapper();

    // ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StringWriter sw = new StringWriter();
    try
    {
      mapper.writeValue(sw, result);
      // System.out.println(sw.toString());

      ZNRecord zn = mapper.readValue(new StringReader(sw.toString()),
          ZNRecord.class);
      System.out.println(result.toString());
      System.out.println(zn.toString());
      AssertJUnit.assertTrue(zn.toString().equalsIgnoreCase(result.toString()));
      System.out.println();
      
      sw= new StringWriter();
      mapper.writeValue(sw, result2);

      ZNRecord zn2 = mapper.readValue(new StringReader(sw.toString()),
          ZNRecord.class);
      System.out.println(result2.toString());
      System.out.println(zn2.toString());
      AssertJUnit.assertTrue(zn2.toString().equalsIgnoreCase(result2.toString()));
      
      sw= new StringWriter();
      mapper.writeValue(sw, result3);
      System.out.println();

      ZNRecord zn3 = mapper.readValue(new StringReader(sw.toString()),
          ZNRecord.class);
      System.out.println(result3.toString());
      System.out.println(zn3.toString());
      AssertJUnit.assertTrue(zn3.toString().equalsIgnoreCase(result3.toString()));
      System.out.println();
      
    } catch (JsonGenerationException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (JsonMappingException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
