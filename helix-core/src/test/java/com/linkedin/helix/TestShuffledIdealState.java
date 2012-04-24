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
package com.linkedin.helix;

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

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.tools.IdealCalculatorByConsistentHashing;
import com.linkedin.helix.tools.IdealStateCalculatorByRush;
import com.linkedin.helix.tools.IdealStateCalculatorByShuffling;

public class TestShuffledIdealState
{
  @Test ()
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
