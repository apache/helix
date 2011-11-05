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
    return false;
  }
  
  public boolean matches(Set<String> anotherTagsMeta)
  {
    return anotherTagsMeta.containsAll(_tagsMeta) || _tagsMeta.containsAll(anotherTagsMeta);
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
    sb.append("{");
    for (String tagMeta : tagMetaList)
    {
      sb.append("\""+tagMeta+"\":");
      sb.append("\""+_tagPairs.get(tagMeta)+"\"");
      if(tagMeta != tagMetaList.get(tagMetaList.size()-1))
      {
        sb.append(",");
      }
    }
    sb.append("}");
    return sb.toString(); 
  }
  
  public String toString()
  {
    return _strRepresentation;
  }
}
