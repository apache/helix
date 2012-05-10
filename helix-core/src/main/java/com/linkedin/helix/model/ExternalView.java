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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;

/**
 * External view is an aggregation (across all instances)
 *  of current states for the partitions in a resource
 */
public class ExternalView extends ZNRecordDecorator
{
  public ExternalView(String resource)
  {
    super(new ZNRecord(resource));
  }

  public ExternalView(ZNRecord record)
  {
    super(record);
  }

  public void setState(String partition, String instance, String state)
  {
    if(_record.getMapField(partition) == null)
    {
      _record.setMapField(partition, new TreeMap<String, String>());
    }
    _record.getMapField(partition).put(instance, state);
  }

  public void setStateMap(String partitionName,
      Map<String, String> currentStateMap)
  {
    _record.setMapField(partitionName, currentStateMap);
  }

  public Set<String> getPartitionSet()
  {
    return _record.getMapFields().keySet();
  }

  public Map<String, String> getStateMap(String partitionName)
  {
    return _record.getMapField(partitionName);
  }

  public String getResourceName()
  {
    return _record.getId();
  }

  @Override
  public boolean isValid()
  {
    return true;
  }
}
