package org.apache.helix.healthcheck;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.log4j.Logger;

import java.util.Map;

public class Stat
{

  private static final Logger _logger = Logger.getLogger(Stat.class);

  public final static String OP_TYPE = "HTTP_OP";
  public final static String MEASUREMENT_TYPE = "MEASUREMENT";
  public final static String RESOURCE_NAME = "RESOURCE_NAME";
  public final static String PARTITION_NAME = "PARTITION_NAME";
  public final static String NODE_NAME = "NODE_NAME";
  public final static String TIMESTAMP = "TIMESTAMP";
  public final static String RETURN_STATUS = "RETURN_STATUS";
  public final static String METRIC_NAME = "METRIC_NAME";
  public final static String AGG_TYPE = "AGG_TYPE";

  public String _opType;
  public String _measurementType;
  public String _resourceName;
  public String _partitionName;
  public String _nodeName;
  public String _returnStatus;
  public String _metricName;
  public String _aggTypeName;
  public String _timestamp;

  public Stat(String opType, String measurementType, String resourceName,
      String partitionName, String nodeName)
  {
    // this(opType, measurementType, resourceName, partitionName, nodeName,
    // null, null, null);
    this(opType, measurementType, resourceName, partitionName, nodeName, null,
        null, null);
  }

  public Stat(String opType, String measurementType, String resourceName,
      String partitionName, String nodeName, String returnStatus,
      String metricName, AggregationType aggType)
  {
    this._opType = opType;
    this._measurementType = measurementType;
    this._resourceName = resourceName;
    this._partitionName = partitionName;
    this._nodeName = nodeName;
    this._returnStatus = returnStatus;
    this._metricName = metricName;
    this._aggTypeName = null;
    if (aggType != null)
    {
      this._aggTypeName = aggType.getName();
    }

    _timestamp = String.valueOf(System.currentTimeMillis());
  }

  public Stat(Map<String, String> in)
  {
    _opType = in.get(OP_TYPE);
    _measurementType = in.get(MEASUREMENT_TYPE);
    _resourceName = in.get(RESOURCE_NAME);
    _partitionName = in.get(PARTITION_NAME);
    _nodeName = in.get(NODE_NAME);
    _timestamp = String.valueOf(System.currentTimeMillis());
  }

  public void setAggType(AggregationType aggType)
  {
    this._aggTypeName = aggType.getName();
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof Stat))
    {
      return false;
    }
    Stat other = (Stat) obj;
    if (!_partitionName.equals(other._partitionName))
    {
      return false;
    }
    if (!_opType.equals(other._opType))
    {
      return false;
    }
    if (!_measurementType.equals(other._measurementType))
    {
      return false;
    }
    if (!_resourceName.equals(other._resourceName))
    {
      return false;
    }
    if (!_nodeName.equals(other._nodeName))
    {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return (_partitionName + _opType + _measurementType + _resourceName + _nodeName)
        .hashCode();
  }

  public void addAlert(long value)
  {
    // TODO Auto-generated method stub

  }

  public String toString()
  {
    return _nodeName + "." + _resourceName + "." + _partitionName + "."
        + _opType + "." + _measurementType + "." + _returnStatus + "."
        + _metricName + "." + _aggTypeName;
  }
}
