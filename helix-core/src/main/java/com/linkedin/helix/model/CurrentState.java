/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.model;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.ZNRecord;

/**
 * Current states of partitions in a resource
 */
public class CurrentState extends HelixProperty
{
  private static Logger LOG = Logger.getLogger(CurrentState.class);

  public enum CurrentStateProperty {
    SESSION_ID, CURRENT_STATE, STATE_MODEL_DEF, STATE_MODEL_FACTORY_NAME, RESOURCE, BUCKET_SIZE
  }

  public CurrentState(String resourceName)
  {
    super(resourceName);
  }

  public CurrentState(ZNRecord record)
  {
    super(record);
  }

  public String getResourceName()
  {
    return _record.getId();
  }

  public Map<String, String> getPartitionStateMap()
  {
    Map<String, String> map = new HashMap<String, String>();
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    for (String partitionName : mapFields.keySet())
    {
      Map<String, String> tempMap = mapFields.get(partitionName);
      if (tempMap != null)
      {
        map.put(partitionName, tempMap.get(CurrentStateProperty.CURRENT_STATE.toString()));
      }
    }
    return map;
  }

  public String getSessionId()
  {
    return _record.getSimpleField(CurrentStateProperty.SESSION_ID.toString());
  }

  public void setSessionId(String sessionId)
  {
    _record.setSimpleField(CurrentStateProperty.SESSION_ID.toString(), sessionId);
  }

  public String getState(String partitionName)
  {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    Map<String, String> mapField = mapFields.get(partitionName);
    if (mapField != null)
    {
      return mapField.get(CurrentStateProperty.CURRENT_STATE.toString());
    }
    return null;
  }

  public void setStateModelDefRef(String stateModelName)
  {
    _record.setSimpleField(CurrentStateProperty.STATE_MODEL_DEF.toString(), stateModelName);
  }

  public String getStateModelDefRef()
  {
    return _record.getSimpleField(CurrentStateProperty.STATE_MODEL_DEF.toString());
  }

  public void setState(String partitionName, String state)
  {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    if (mapFields.get(partitionName) == null)
    {
      mapFields.put(partitionName, new TreeMap<String, String>());
    }
    mapFields.get(partitionName).put(CurrentStateProperty.CURRENT_STATE.toString(), state);
  }

  public void setStateModelFactoryName(String factoryName)
  {
    _record.setSimpleField(CurrentStateProperty.STATE_MODEL_FACTORY_NAME.toString(), factoryName);
  }

  public String getStateModelFactoryName()
  {
    return _record.getSimpleField(CurrentStateProperty.STATE_MODEL_FACTORY_NAME.toString());
  }

  public int getBucketSize()
  {
    String bucketSizeStr = _record.getSimpleField(CurrentStateProperty.BUCKET_SIZE.toString());
    int bucketSize = 0;
    if (bucketSizeStr != null)
    {
      try
      {
        bucketSize = Integer.parseInt(bucketSizeStr);
      } catch (NumberFormatException e)
      {
        // OK
      }
    }
    return bucketSize;
  }

  public void setBucketSize(int bucketSize)
  {
    if (bucketSize > 0)
    {
      _record.setSimpleField(CurrentStateProperty.BUCKET_SIZE.toString(), "" + bucketSize);
    }
  }
  
  @Override
  public boolean isValid()
  {
    if (getStateModelDefRef() == null)
    {
      LOG.error("Current state does not contain state model ref. id:" + getResourceName());
      return false;
    }
    if (getSessionId() == null)
    {
      LOG.error("CurrentState does not contain session id, id : " + getResourceName());
      return false;
    }
    return true;
  }

}
