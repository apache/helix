package com.linkedin.clustermanager.monitoring;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SensorContextTags
{
  final Map<String, String> _tagPairs;
  final Set<String> _tagsMeta;
  final String _strRepresentation;
  
  public SensorContextTags(Map<String, String> tags)
  {
    _tagPairs = new HashMap<String, String>();
    _tagPairs.putAll(tags);
    _tagsMeta  = tags.keySet();
    _strRepresentation = toStringInternal();
  }
  
  public Map<String, String> getTags()
  {
    return _tagPairs;
  }
  
  public Set<String> getTagsMeta()
  {
     return _tagsMeta;
  }
  
  public boolean matches(String format)
  {
    return new SensorTagFilter(SensorContextTags.fromString(format).getTags()).matchs(this);
  }
  
  public boolean matches(Set<String> anotherTagsMeta)
  {
    return anotherTagsMeta.containsAll(_tagsMeta) || _tagsMeta.containsAll(anotherTagsMeta);
  }
  
  public boolean containsTags(Map<String, String> anotherTag)
  {
    for(String key : anotherTag.keySet())
    {
      if(!_tagPairs.containsKey(key) || !_tagPairs.get(key).equalsIgnoreCase(anotherTag.get(key)))
      {
        return false;
      }
    }
    return true;
  }
  
  String toStringInternal()
  {
    List<String> tagMetaList = new ArrayList<String>();
    for(String s : _tagsMeta)
    {
      tagMetaList.add(s);
    }
    Collections.sort(tagMetaList);
    StringBuffer sb = new StringBuffer();
    for (String tagMeta : tagMetaList)
    {
      sb.append(tagMeta);
      sb.append("=");
      sb.append(_tagPairs.get(tagMeta));
      if(tagMeta != tagMetaList.get(tagMetaList.size()-1))
      {
        sb.append(", ");
      }
    }    
    return sb.toString(); 
  }
  
  @Override
  public String toString()
  {
    return _strRepresentation;
  }
  
  @Override
  public int hashCode()
  {
    return _strRepresentation.hashCode();
  }
  
  public static SensorContextTags fromString(String str)
  {
    HashMap<String, String> result = new HashMap<String, String>();
    String[] pairs = str.split(",");
    for(String pair : pairs)
    {
      int pos = pair.indexOf("=");
      String key = pair.substring(0,pos).trim();
      String value = pair.substring(pos+1).trim();
      result.put(key, value);
    }
    return new SensorContextTags(result);
  }
}
