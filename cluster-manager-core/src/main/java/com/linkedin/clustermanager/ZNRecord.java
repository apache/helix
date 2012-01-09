package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import com.linkedin.clustermanager.ZNRecordDelta.MERGEOPERATION;

/**
 * Generic Record Format to store data at a Node This can be used to store simpleFields
 * mapFields listFields
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZNRecord
{
  static Logger _logger = Logger.getLogger(ZNRecord.class);

  private final String id;

  /**
   * We don't want the _deltaList to be serialized and deserialized
   */
  private List<ZNRecordDelta> _deltaList = new ArrayList<ZNRecordDelta>();

  private Map<String, String> simpleFields;
  private Map<String, Map<String, String>> mapFields;
  private Map<String, List<String>> listFields;

  /**
   * the version field of zookeeper Stat
   */
  private int _version;

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
    this(record, record.getId());
  }

  public ZNRecord(ZNRecord record, String id)
  {
    this(id);
    simpleFields.putAll(record.getSimpleFields());
    mapFields.putAll(record.getMapFields());
    listFields.putAll(record.getListFields());
  }

  public ZNRecord(ZNRecord record, int version)
  {
    this(record);
    _version = version;
  }

  @JsonIgnore(true)
  public void setDeltaList(List<ZNRecordDelta> deltaList)
  {
    _deltaList = deltaList;
  }

  @JsonIgnore(true)
  public List<ZNRecordDelta> getDeltaList()
  {
    return _deltaList;
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

  @Override
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
   * merge functionality is used to merge multiple znrecord into a single one. This will
   * make use of the id of each ZNRecord and append it to every key thus making key
   * unique. This is needed to optimize on the watches.
   *
   * @param record
   */
  public void merge(ZNRecord record)
  {
    if (record == null)
    {
      return;
    }

    if (record.getDeltaList().size() > 0)
    {
      _logger.info("Merging with delta list, recordId = " + id + " other:"
          + record.getId());
      merge(record.getDeltaList());
      return;
    }
    simpleFields.putAll(record.simpleFields);
    for (String key : record.mapFields.keySet())
    {
      Map<String, String> map = mapFields.get(key);
      if (map != null)
      {
        map.putAll(record.mapFields.get(key));
      }
      else
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
      }
      else
      {
        listFields.put(key, record.listFields.get(key));
      }
    }
  }

  void merge(ZNRecordDelta delta)
  {
    if (delta.getMergeOperation() == MERGEOPERATION.ADD)
    {
      merge(delta.getRecord());
    }
    else if (delta.getMergeOperation() == MERGEOPERATION.SUBSTRACT)
    {
      subtract(delta.getRecord());
    }
  }

  void merge(List<ZNRecordDelta> deltaList)
  {
    for (ZNRecordDelta delta : deltaList)
    {
      merge(delta);
    }
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof ZNRecord))
    {
      return false;
    }
    ZNRecord that = (ZNRecord) obj;
    if (this.getSimpleFields().size() != that.getSimpleFields().size())
    {
      return false;
    }
    if (this.getMapFields().size() != that.getMapFields().size())
    {
      return false;
    }
    if (this.getListFields().size() != that.getListFields().size())
    {
      return false;
    }
    if (!this.getSimpleFields().equals(that.getSimpleFields()))
    {
      return false;
    }
    if (!this.getMapFields().equals(that.getMapFields()))
    {
      return false;
    }
    if (!this.getListFields().equals(that.getListFields()))
    {
      return false;
    }

    return true;
  }

  /**
   *  Note: does not support subtract in each list in list fields
   *  or map in mapFields
   */
  public void subtract(ZNRecord value)
  {
    for (String key : value.getSimpleFields().keySet())
    {
      if (simpleFields.containsKey(key))
      {
        simpleFields.remove(key);
      }
    }

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

  @JsonIgnore(true)
  public int getVersion()
  {
    return _version;
  }

  @JsonIgnore(true)
  public void setVersion(int version)
  {
    _version = version;
  }

}
