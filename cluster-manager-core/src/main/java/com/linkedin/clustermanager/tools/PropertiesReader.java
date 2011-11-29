package com.linkedin.clustermanager.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.testng.Assert;

public class PropertiesReader
{
  private final Properties _properties = new Properties();

  public PropertiesReader(String propertyFileName)
  {
    try
    {
      InputStream stream = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream(propertyFileName);
      _properties.load(stream);
    }
    catch (IOException e)
    {
      Assert.fail("could not open properties file:" + propertyFileName, e);
    }
  }

  public String getProperty(String key)
  {
    String value = _properties.getProperty(key);
    if (value == null)
    {
      throw new IllegalArgumentException("no property exist for key:" + key);
    }

    return value;
  }
}
