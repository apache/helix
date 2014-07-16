package org.apache.helix.manager.zk;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.GroupCommit;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZNRecordAssembler;
import org.apache.helix.ZNRecordBucketizer;
import org.apache.helix.ZNRecordUpdater;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

public class ZKHelixDataAccessor implements HelixDataAccessor {
  private static Logger LOG = Logger.getLogger(ZKHelixDataAccessor.class);
  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;
  final InstanceType _instanceType;
  private final String _clusterName;
  private final Builder _propertyKeyBuilder;
  private final GroupCommit _groupCommit = new GroupCommit();
  String _zkPropertyTransferSvcUrl = null;

  public ZKHelixDataAccessor(String clusterName, BaseDataAccessor<ZNRecord> baseDataAccessor) {
    this(clusterName, null, baseDataAccessor);
  }

  public ZKHelixDataAccessor(String clusterName, InstanceType instanceType,
      BaseDataAccessor<ZNRecord> baseDataAccessor) {
    _clusterName = clusterName;
    _instanceType = instanceType;
    _baseDataAccessor = baseDataAccessor;
    _propertyKeyBuilder = new PropertyKey.Builder(_clusterName);
  }

  @Override
  public <T extends HelixProperty> boolean createProperty(PropertyKey key, T value) {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);
    boolean success = false;
    switch (type) {
    case STATEMODELDEFS:
      if (value != null && value.isValid()) {
        success = _baseDataAccessor.create(path, value.getRecord(), options);
      }
      break;
    default:
      success = _baseDataAccessor.create(path, value == null ? null : value.getRecord(), options);
      break;
    }
    return success;
  }

  @Override
  public <T extends HelixProperty> boolean setProperty(PropertyKey key, T value) {
    PropertyType type = key.getType();
    if (!value.isValid()) {
      throw new HelixException("The ZNRecord for " + type + " is not valid.");
    }

    String path = key.getPath();
    int options = constructOptions(type);

    boolean success = false;
    switch (type) {
    case RESOURCEASSIGNMENTS:
    case IDEALSTATES:
    case EXTERNALVIEW:
      // check if bucketized
      if (value.getBucketSize() > 0) {
        // set parent node
        ZNRecord metaRecord = new ZNRecord(value.getId());
        metaRecord.setSimpleFields(value.getRecord().getSimpleFields());
        success = _baseDataAccessor.set(path, metaRecord, options);
        if (success) {
          ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(value.getBucketSize());

          Map<String, ZNRecord> map = bucketizer.bucketize(value.getRecord());
          List<String> paths = new ArrayList<String>();
          List<ZNRecord> bucketizedRecords = new ArrayList<ZNRecord>();
          for (String bucketName : map.keySet()) {
            paths.add(path + "/" + bucketName);
            bucketizedRecords.add(map.get(bucketName));
          }

          // TODO: set success accordingly
          _baseDataAccessor.setChildren(paths, bucketizedRecords, options);
        }
      } else {
        success = _baseDataAccessor.set(path, value.getRecord(), options);
      }
      break;
    default:
      success = _baseDataAccessor.set(path, value.getRecord(), options);
      break;
    }
    return success;
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, T value) {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);

    boolean success = false;
    switch (type) {
    case CURRENTSTATES:
      success = _groupCommit.commit(_baseDataAccessor, options, path, value.getRecord(), true);
      break;
    case STATUSUPDATES:
      if (LOG.isTraceEnabled()) {
        LOG.trace("Update status. path: " + key.getPath() + ", record: " + value.getRecord());
      }
      break;
    default:
      success = _baseDataAccessor.update(path, new ZNRecordUpdater(value.getRecord()), options);
      break;
    }
    return success;
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys) {
    if (keys == null || keys.size() == 0) {
      return Collections.emptyList();
    }

    List<T> childValues = new ArrayList<T>();

    // read all records
    List<String> paths = new ArrayList<String>();
    for (PropertyKey key : keys) {
      paths.add(key.getPath());
    }
    List<ZNRecord> children = _baseDataAccessor.get(paths, null, 0);

    // check if bucketized
    for (int i = 0; i < keys.size(); i++) {
      PropertyKey key = keys.get(i);
      ZNRecord record = children.get(i);

      PropertyType type = key.getType();
      String path = key.getPath();
      int options = constructOptions(type);
      // ZNRecord record = null;

      switch (type) {
      case RESOURCEASSIGNMENTS:
      case CURRENTSTATES:
      case IDEALSTATES:
      case EXTERNALVIEW:
        // check if bucketized
        if (record != null) {
          HelixProperty property = new HelixProperty(record);

          int bucketSize = property.getBucketSize();
          if (bucketSize > 0) {
            List<ZNRecord> childRecords = _baseDataAccessor.getChildren(path, null, options);
            ZNRecord assembledRecord = new ZNRecordAssembler().assemble(childRecords);

            // merge with parent node value
            if (assembledRecord != null) {
              record.getSimpleFields().putAll(assembledRecord.getSimpleFields());
              record.getListFields().putAll(assembledRecord.getListFields());
              record.getMapFields().putAll(assembledRecord.getMapFields());
            }
          }
        }
        break;
      default:
        break;
      }

      @SuppressWarnings("unchecked")
      T t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
      childValues.add(t);
    }

    return childValues;
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);
    ZNRecord record = null;
    try {
      Stat stat = new Stat();
      record = _baseDataAccessor.get(path, stat, options);
      if (record != null) {
        record.setCreationTime(stat.getCtime());
        record.setModifiedTime(stat.getMtime());
      }
    } catch (ZkNoNodeException e) {
      // OK
    }

    switch (type) {
    case RESOURCEASSIGNMENTS:
    case CURRENTSTATES:
    case IDEALSTATES:
    case EXTERNALVIEW:
      // check if bucketized
      if (record != null) {
        HelixProperty property = new HelixProperty(record);

        int bucketSize = property.getBucketSize();
        if (bucketSize > 0) {
          List<ZNRecord> childRecords = _baseDataAccessor.getChildren(path, null, options);
          ZNRecord assembledRecord = new ZNRecordAssembler().assemble(childRecords);

          // merge with parent node value
          if (assembledRecord != null) {
            record.getSimpleFields().putAll(assembledRecord.getSimpleFields());
            record.getListFields().putAll(assembledRecord.getListFields());
            record.getMapFields().putAll(assembledRecord.getMapFields());
          }
        }
      }
      break;
    default:
      break;
    }

    @SuppressWarnings("unchecked")
    T t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
    return t;
  }

  @Override
  public boolean removeProperty(PropertyKey key) {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);

    return _baseDataAccessor.remove(path, options);
  }

  @Override
  public List<String> getChildNames(PropertyKey key) {
    PropertyType type = key.getType();
    String parentPath = key.getPath();
    int options = constructOptions(type);
    List<String> childNames = _baseDataAccessor.getChildNames(parentPath, options);
    if (childNames == null) {
      childNames = Collections.emptyList();
    }
    return childNames;
  }

  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key) {
    PropertyType type = key.getType();
    String parentPath = key.getPath();
    int options = constructOptions(type);
    List<T> childValues = new ArrayList<T>();

    List<ZNRecord> children = _baseDataAccessor.getChildren(parentPath, null, options);
    if (children != null) {
      for (ZNRecord record : children) {
        switch (type) {
        case RESOURCEASSIGNMENTS:
        case CURRENTSTATES:
        case IDEALSTATES:
        case EXTERNALVIEW:
          if (record != null) {
            HelixProperty property = new HelixProperty(record);

            int bucketSize = property.getBucketSize();
            if (bucketSize > 0) {
              // TODO: fix this if record.id != pathName
              String childPath = parentPath + "/" + record.getId();
              List<ZNRecord> childRecords = _baseDataAccessor.getChildren(childPath, null, options);
              ZNRecord assembledRecord = new ZNRecordAssembler().assemble(childRecords);

              // merge with parent node value
              if (assembledRecord != null) {
                record.getSimpleFields().putAll(assembledRecord.getSimpleFields());
                record.getListFields().putAll(assembledRecord.getListFields());
                record.getMapFields().putAll(assembledRecord.getMapFields());
              }
            }
          }

          break;
        default:
          break;
        }

        if (record != null) {
          @SuppressWarnings("unchecked")
          T t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
          childValues.add(t);
        }
      }
    }
    return childValues;
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key) {
    PropertyType type = key.getType();
    String parentPath = key.getPath();
    int options = constructOptions(type);
    List<T> children = getChildValues(key);
    Map<String, T> childValuesMap = new HashMap<String, T>();
    for (T t : children) {
      childValuesMap.put(t.getRecord().getId(), t);
    }
    return childValuesMap;

  }

  @Override
  public Builder keyBuilder() {
    return _propertyKeyBuilder;
  }

  private int constructOptions(PropertyType type) {
    int options = 0;
    if (type.isPersistent()) {
      options = options | AccessOption.PERSISTENT;
    } else {
      options = options | AccessOption.EPHEMERAL;
    }

    return options;
  }

  @Override
  public <T extends HelixProperty> boolean[] createChildren(List<PropertyKey> keys, List<T> children) {
    // TODO: add validation
    int options = -1;
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < keys.size(); i++) {
      PropertyKey key = keys.get(i);
      PropertyType type = key.getType();
      String path = key.getPath();
      paths.add(path);
      HelixProperty value = children.get(i);
      records.add(value.getRecord());
      options = constructOptions(type);
    }
    return _baseDataAccessor.createChildren(paths, records, options);
  }

  @Override
  public <T extends HelixProperty> boolean[] setChildren(List<PropertyKey> keys, List<T> children) {
    int options = -1;
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();

    List<List<String>> bucketizedPaths =
        new ArrayList<List<String>>(Collections.<List<String>> nCopies(keys.size(), null));
    List<List<ZNRecord>> bucketizedRecords =
        new ArrayList<List<ZNRecord>>(Collections.<List<ZNRecord>> nCopies(keys.size(), null));

    for (int i = 0; i < keys.size(); i++) {
      PropertyKey key = keys.get(i);
      PropertyType type = key.getType();
      String path = key.getPath();
      paths.add(path);
      options = constructOptions(type);

      HelixProperty value = children.get(i);

      switch (type) {
      case RESOURCEASSIGNMENTS:
      case EXTERNALVIEW:
        if (value.getBucketSize() == 0) {
          records.add(value.getRecord());
        } else {
          _baseDataAccessor.remove(path, options);

          ZNRecord metaRecord = new ZNRecord(value.getId());
          metaRecord.setSimpleFields(value.getRecord().getSimpleFields());
          records.add(metaRecord);

          ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(value.getBucketSize());

          Map<String, ZNRecord> map = bucketizer.bucketize(value.getRecord());
          List<String> childBucketizedPaths = new ArrayList<String>();
          List<ZNRecord> childBucketizedRecords = new ArrayList<ZNRecord>();
          for (String bucketName : map.keySet()) {
            childBucketizedPaths.add(path + "/" + bucketName);
            childBucketizedRecords.add(map.get(bucketName));
          }
          bucketizedPaths.set(i, childBucketizedPaths);
          bucketizedRecords.set(i, childBucketizedRecords);
        }
        break;
      case STATEMODELDEFS:
        if (value.isValid()) {
          records.add(value.getRecord());
        }
        break;
      default:
        records.add(value.getRecord());
        break;
      }
    }

    // set non-bucketized nodes or parent nodes of bucketized nodes
    boolean success[] = _baseDataAccessor.setChildren(paths, records, options);

    // set bucketized nodes
    List<String> allBucketizedPaths = new ArrayList<String>();
    List<ZNRecord> allBucketizedRecords = new ArrayList<ZNRecord>();

    for (int i = 0; i < keys.size(); i++) {
      if (success[i] && bucketizedPaths.get(i) != null) {
        allBucketizedPaths.addAll(bucketizedPaths.get(i));
        allBucketizedRecords.addAll(bucketizedRecords.get(i));
      }
    }

    // TODO: set success accordingly
    _baseDataAccessor.setChildren(allBucketizedPaths, allBucketizedRecords, options);
    return success;
  }

  @Override
  public BaseDataAccessor<ZNRecord> getBaseDataAccessor() {
    return _baseDataAccessor;
  }

  @Override
  public <T extends HelixProperty> boolean[] updateChildren(List<String> paths,
      List<DataUpdater<ZNRecord>> updaters, int options) {
    return _baseDataAccessor.updateChildren(paths, updaters, options);
  }
}
