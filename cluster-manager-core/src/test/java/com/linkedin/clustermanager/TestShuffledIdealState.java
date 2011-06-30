package com.linkedin.clustermanager;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Assert;
import org.testng.annotations.*;


import com.linkedin.clustermanager.tools.IdealStateCalculatorByShuffling;

public class TestShuffledIdealState 
{
    // public static void main(String[] args)
    @Test
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
            AssertJUnit.assertTrue(zn.toString().equalsIgnoreCase(
                    result.toString()));
        }
        catch (JsonGenerationException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (JsonMappingException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
