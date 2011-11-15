package com.linkedin.clustermanager.monitoring;

import java.util.HashMap;
import java.util.Map;

public class SensorTagFilter extends SensorContextTags
{
  public SensorTagFilter(Map<String, String> tagFilter)
  {
    super(tagFilter);
  }
  
  public boolean matchs(SensorContextTags tags)
  {
    for(String key : _tagPairs.keySet())
    {
      if(!tags.getTags().containsKey(key))
      {
        return false;
      }
      if(!_tagPairs.get(key).equals("*"))
      {
        if(!_tagPairs.get(key).equalsIgnoreCase(tags.getTags().get(key)))
        {
          return false;
        }
      }
    }
    return true;
  }
  
  public SensorContextTags getFilteredTags(SensorContextTags tags)
  {
    if(!matchs(tags))
    {
      return null;
    }
    Map<String, String> resultTags = new HashMap<String, String>();
    for(String key : _tagPairs.keySet())
    {
      resultTags.put(key, tags.getTags().get(key));
    }
    return new SensorContextTags(resultTags);
  }
  
  public static SensorTagFilter fromString(String str)
  {
    SensorContextTags tags = SensorContextTags.fromString(str);
    if(tags!=null)
    {
      return new SensorTagFilter(tags.getTags());
    }
    return null;
  }
}
