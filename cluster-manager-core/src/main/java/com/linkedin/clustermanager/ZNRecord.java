package com.linkedin.clustermanager;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Generic Record Format to store data at a Node This can be used to store
 * simpleFields mapFields listFields
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZNRecord
{

  private final String id;
  private Map<String, String> simpleFields;
  private Map<String, Map<String, String>> mapFields;
  private Map<String, List<String>> listFields;

  @JsonCreator
  public ZNRecord(@JsonProperty("id") String id)
  {
    this.id = id;
    simpleFields = new TreeMap<String, String>();
    mapFields = new TreeMap<String, Map<String, String>>();
    listFields = new TreeMap<String, List<String>>();
  }

  public ZNRecord(ZNRecord record)
  {
    this(record.getId());
    simpleFields.putAll(record.getSimpleFields());
    mapFields.putAll(record.getMapFields());
    listFields.putAll(record.getListFields());
  }
  
  public ZNRecord(ZNRecord record, String id)
  {
    this(id);
    simpleFields.putAll(record.getSimpleFields());
    mapFields.putAll(record.getMapFields());
    listFields.putAll(record.getListFields());
  }

  @JsonProperty
  public Map<String, String> getSimpleFields()
  {
    return simpleFields;
  }

  @JsonProperty
  public void setSimpleFields(Map<String, String> simpleFields)
  {
    this.simpleFields = simpleFields;
  }

  @JsonProperty
  public Map<String, Map<String, String>> getMapFields()
  {
    return mapFields;
  }

  @JsonProperty
  public void setMapFields(Map<String, Map<String, String>> mapFields)
  {
    this.mapFields = mapFields;
  }

  @JsonProperty
  public Map<String, List<String>> getListFields()
  {
    return listFields;
  }

  @JsonProperty
  public void setListFields(Map<String, List<String>> listFields)
  {
    this.listFields = listFields;
  }

  @JsonProperty
  public void setSimpleField(String k, String v)
  {
    simpleFields.put(k, v);
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  public void setMapField(String k, Map<String, String> v)
  {
    mapFields.put(k, v);
  }

  public void setListField(String k, List<String> v)
  {
    listFields.put(k, v);
  }

  public String getSimpleField(String k)
  {
    return simpleFields.get(k);
  }

  public Map<String, String> getMapField(String k)
  {
    return mapFields.get(k);
  }

  public List<String> getListField(String k)
  {
    return listFields.get(k);
  }

  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    if (simpleFields != null)
    {
      sb.append(simpleFields);
    }
    if (mapFields != null)
    {
      sb.append(mapFields);
    }
    if (listFields != null)
    {
      sb.append(listFields);
    }
    return sb.toString();
  }

  /**
   * merge functionality is used to merge multiple znrecord into a single one.
   * This will make use of the id of each ZNRecord and append it to every key
   * thus making key unique. This is needed to optimize on the watches.
   * 
   * @param record
   */
  public void merge(ZNRecord record)
  {
    if(record == null){
      return;
    }
    simpleFields.putAll(record.simpleFields);
    for (String key : record.mapFields.keySet())
    {
      Map<String, String> map = mapFields.get(key);
      if (map != null)
      {
        map.putAll(record.mapFields.get(key));
      } else
      {
        mapFields.put(key, record.mapFields.get(key));
      }
    }
    for (String key : record.listFields.keySet())
    {
      List<String> list = listFields.get(key);
      if (list != null)
      {
        list.addAll(record.listFields.get(key));
      } else
      {
        listFields.put(key, record.listFields.get(key));
      }
    }
  }
  @Override
  public boolean equals(Object obj)
  {
    if(!(obj instanceof ZNRecord)){
      return false;
    }
    ZNRecord that = (ZNRecord) obj;
    if(this.getSimpleFields().size()!= that.getSimpleFields().size()){
      return false;
    }
    if(this.getMapFields().size()!= that.getMapFields().size()){
      return false;
    }
    if(this.getListFields().size()!= that.getListFields().size()){
      return false;
    }
    if(!this.getSimpleFields().equals(that.getSimpleFields())){
      return false;
    }
    if(!this.getMapFields().equals(that.getMapFields())){
      return false;
    }
    if(!this.getListFields().equals(that.getListFields())){
      return false;
    }
    
    return true;
  }
  public void substract(ZNRecord value)
  {
    for (String key : value.getSimpleFields().keySet())
    {
      if (simpleFields.containsKey(key))
      {
        simpleFields.remove(key);
      }
    }
    // Note: does not support substract in each list in list fields
    // or map in mapFields
    for (String key : value.getListFields().keySet())
    {
      if (listFields.containsKey(key))
      {
        listFields.remove(key);
      }
    }

    for (String key : value.getMapFields().keySet())
    {
      if (mapFields.containsKey(key))
      {
        mapFields.remove(key);
      }
    }
  }
}
