package com.linkedin.helix.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class StringTemplate
{
  private static Logger LOG = Logger.getLogger(StringTemplate.class);

  Map<Enum, Map<Integer, String>> templateMap = new HashMap<Enum, Map<Integer, String>>();
  static Pattern pattern = Pattern.compile("(\\{.+?\\})");

  public void addEntry(Enum type, int numKeys, String template)
  {
    if (!templateMap.containsKey(type))
    {
      templateMap.put(type, new HashMap<Integer, String>());
    }
    LOG.trace("Add template for type: " + type.name() + ", arguments: " + numKeys
        + ", template: " + template);
    templateMap.get(type).put(numKeys, template);
  }

  public String instantiate(Enum type, String... keys)
  {
    if (keys == null)
    {
      keys = new String[] {};
    }

    String template = null;
    if (templateMap.containsKey(type))
    {
      template = templateMap.get(type).get(keys.length);
    }

    String result = null;

    if (template != null)
    {
      result = template;
      Matcher matcher = pattern.matcher(template);
      int count = 0;
      while (matcher.find())
      {
        String var = matcher.group();
        result = result.replace(var, keys[count]);
        count++;
      }
    }

    if (result == null || result.indexOf('{') > -1 || result.indexOf('}') > -1)
    {
      String errMsg = "Unable to instantiate template: " + template
          + " using keys: " + Arrays.toString(keys);
      LOG.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }

    return result;
  }

}
