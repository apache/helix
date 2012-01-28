package com.linkedin.helix;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGetProperty
{
  @Test
  public void testGetProperty()
  {
    String version;
    Properties props = new Properties();

    try
    {
      InputStream stream = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream("cluster-manager-version.properties");
      props.load(stream);
      version = props.getProperty("clustermanager.version");
      Assert.assertNotNull(version);
      System.out.println("cluster-manager-version:" + version);
    }
    catch (IOException e)
    {
      // e.printStackTrace();
      Assert.fail("could not open cluster-manager-version.properties. ", e);
    }
  }
}
