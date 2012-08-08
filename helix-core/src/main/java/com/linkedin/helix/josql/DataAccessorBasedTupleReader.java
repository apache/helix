package com.linkedin.helix.josql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;

public class DataAccessorBasedTupleReader implements ZNRecordQueryProcessor.ZNRecordTupleReader
{
  Map<String , List<ZNRecord>> _cache = new HashMap<String, List<ZNRecord>>();
  private HelixDataAccessor _dataAccessor;
  private String _clusterName;

  public DataAccessorBasedTupleReader(HelixDataAccessor dataAccessor, String clusterName)
  {
    _dataAccessor = dataAccessor;
    _clusterName = clusterName;
  }

  @Override
  public List<ZNRecord> get(String path) throws Exception
  {
    //first check cache
    if(_cache.containsKey(path))
    {
      return _cache.get(path);
    }
    
    String[] tmp = path.split("/");
    if(tmp.length == 0 || tmp[0].equals("*"))
    {
      throw new Exception("Unable to read " + path);
    }
    
    PropertyType parentProperty = null;
    try
    {
      parentProperty = PropertyType.valueOf(tmp[0]);
    }
    catch(Exception e)
    {
      throw new Exception("Unable to translate " + tmp[0] + " into a valid property type", e);
    }
    
    boolean pathHasWildcard = path.contains("*");
    String childKeys[] = new String[tmp.length-1];
    for(int i = 1; i < tmp.length; i++)
    {
      childKeys[i-1] = tmp[i];
    }
    
    List<ZNRecord> ret = null;
    String parentPath = PropertyPathConfig.getPath(parentProperty, _clusterName);

    if(pathHasWildcard)
    {
      List<String> paths = expandWildCards(parentProperty, childKeys);
      ret = new ArrayList<ZNRecord>();
      for(String expandedPath : paths)
      {
        String fullPath = parentPath + "/" + expandedPath;
        ZNRecord record = _dataAccessor.getBaseDataAccessor().get(fullPath, null, 0);
        if(record != null)
        {
          ret.add(record);
        }
      }
    }
    else
    {
      String propertyPath = PropertyPathConfig.getPath(parentProperty, _clusterName, childKeys);
      ret = _dataAccessor.getBaseDataAccessor().getChildren(propertyPath, null, 0);
      //ret = _dataAccessor.getChildValues(parentProperty, childKeys);
      if(ret.size() == 0) //could be a leaf node - try accessing property directly
      {
        String fullPath = parentPath;
        for(String childKey : childKeys)
        {
          fullPath += "/" + childKey;
        }
        ZNRecord record = _dataAccessor.getBaseDataAccessor().get(fullPath, null, 0);
        if(record != null)
        {
          ret = Arrays.asList(record);
        }
      }
    }
    
    _cache.put(path, ret);
    return ret;
  }
  
  private List<String> expandWildCards(PropertyType parentProperty, String[] pathElements)
  {
    if(pathElements.length == 0)
    {
      return Collections.emptyList();
    }
    
    String path = "";
    for (int i = 0; i < pathElements.length; i++)
    {
      path += pathElements[i];
      if(i != pathElements.length -1)
      {
        path += "/";
      }
    }
    if(!path.contains("*"))
    {
      return Arrays.asList(path);
    }
    else
    {
      List<String> prefix = new ArrayList<String>();
  
      for (int i = 0; i < pathElements.length; i++)
      {
        if(!pathElements[i].equals("*"))
        {
          prefix.add(pathElements[i]);
        }
        else
        {
          List<String> ret = new ArrayList<String>();
          Set<String> childNames = new HashSet<String>();
          //List<String> childNamesList = _dataAccessor.getChildNames(parentProperty, prefix.toArray(new String[0]));
          String parentPath = PropertyPathConfig.getPath(parentProperty, _clusterName,prefix.toArray(new String[0]));
          List<String> childNamesList = _dataAccessor.getBaseDataAccessor().getChildNames(parentPath, 0);
          childNames.addAll(childNamesList);
          for(String child : childNames)
          {
            pathElements[i] = child;
            ret.addAll(expandWildCards(parentProperty, pathElements));
          }
          return ret;
        }
      }
    }
    
    return Collections.emptyList();
  }

  public void setTempTable(String path, List<ZNRecord> table)
  {
    _cache.put(path, table);
  }

  @Override
  public void reset()
  {
    _cache.clear();
  }
}
