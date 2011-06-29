package com.linkedin.clustermanager.model;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Generic Record Format to store data at a Node This can be used to store
 * simpleFields mapFields listFields
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZNRecord
{

    public String id;
    public Map<String, String> simpleFields;
    public Map<String, Map<String, String>> mapFields;
    public Map<String, List<String>> listFields;

    public ZNRecord()
    {
        simpleFields = new TreeMap<String, String>();
        mapFields = new TreeMap<String, Map<String, String>>();
        listFields = new TreeMap<String, List<String>>();
    }

    public ZNRecord(ZNRecord record)
    {
        this();
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

    @JsonProperty
    public void setId(String id)
    {
        this.id = id;
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
     * @param record
     */
    public void merge(ZNRecord record)
    {
    	this.simpleFields.putAll(record.simpleFields);
    	this.mapFields.putAll(record.mapFields);
    	this.listFields.putAll(record.listFields);
    	
    }

    public ZNRecord extract(String key)
    {
        return this;
    }
}
