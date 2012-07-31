package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.GroupCommit;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordAssembler;
import com.linkedin.helix.ZNRecordUpdater;
import com.linkedin.helix.controller.restlet.ZNRecordUpdate;
import com.linkedin.helix.controller.restlet.ZNRecordUpdate.OpCode;
import com.linkedin.helix.controller.restlet.ZkPropertyTransferClient;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.LiveInstance;

public class ZKHelixDataAccessor implements HelixDataAccessor, ControllerChangeListener
{
  private static Logger                    LOG                       =
                                                                         Logger.getLogger(ZKHelixDataAccessor.class);
  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;
  private final String                     _clusterName;
  private final Builder                    _propertyKeyBuilder;
  final ZkPropertyTransferClient           _zkPropertyTransferClient;
  private final GroupCommit                _groupCommit              = new GroupCommit();
  String                                   _zkPropertyTransferSvcUrl = null;

  public ZKHelixDataAccessor(String clusterName,
                             BaseDataAccessor<ZNRecord> baseDataAccessor)
  {
    _clusterName = clusterName;
    _baseDataAccessor = baseDataAccessor;
    _propertyKeyBuilder = new PropertyKey.Builder(_clusterName);
    _zkPropertyTransferClient =
        new ZkPropertyTransferClient(ZkPropertyTransferClient.DEFAULT_MAX_CONCURRENTTASKS);
  }

  @Override
  public <T extends HelixProperty> boolean createProperty(PropertyKey key, T value)
  {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);
    return _baseDataAccessor.create(path, value.getRecord(), options);
  }

  @Override
  public <T extends HelixProperty> boolean setProperty(PropertyKey key, T value)
  {
    PropertyType type = key.getType();
    if (!value.isValid())
    {
      throw new HelixException("The ZNRecord for " + type + " is not valid.");
    }

    String path = key.getPath();
    int options = constructOptions(type);

    if (type.usePropertyTransferServer() && getPropertyTransferUrl() != null)
    {
      ZNRecordUpdate update = new ZNRecordUpdate(path, OpCode.SET, value.getRecord());
      _zkPropertyTransferClient.sendZNRecordUpdate(update, getPropertyTransferUrl());
      return true;
    }

    return _baseDataAccessor.set(path, value.getRecord(), options);
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, T value)
  {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);

    boolean success = false;
    switch (type)
    {
    case CURRENTSTATES:
      success = _groupCommit.commit(_baseDataAccessor, path, value.getRecord());
      break;
    default:
      if (type.usePropertyTransferServer())
      {
        if (getPropertyTransferUrl() != null)
        {
          ZNRecordUpdate update = new ZNRecordUpdate(path, OpCode.UPDATE, value.getRecord());
          _zkPropertyTransferClient.sendZNRecordUpdate(update, getPropertyTransferUrl());

          return true;
        }
        else
        {
          LOG.debug("getPropertyTransferUrl is null, skip updating the value");
          // TODO: consider skip the write operation
          return true;
        }
      }
      success =
          _baseDataAccessor.update(path, new ZNRecordUpdater(value.getRecord()), options);
      break;
    }
    return success;
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys)
  {
    if (keys == null || keys.size() == 0)
    {
      return Collections.emptyList();
    }

    List<T> childValues = new ArrayList<T>();

    // read all records
    List<String> paths = new ArrayList<String>();
    for (PropertyKey key : keys)
    {
      paths.add(key.getPath());
    }
    List<ZNRecord> children = _baseDataAccessor.get(paths, null, 0);

    // check if bucketized
    for (int i = 0; i < keys.size(); i++)
    {
      PropertyKey key = keys.get(i);
      ZNRecord record = children.get(i);

      PropertyType type = key.getType();
      String path = key.getPath();
      int options = constructOptions(type);
      // ZNRecord record = null;

      switch (type)
      {
      case CURRENTSTATES:
        // check if bucketized
        if (record != null)
        {
          CurrentState curState = new CurrentState(record);
          int bucketSize = curState.getBucketSize();
          if (bucketSize > 0)
          {
            List<ZNRecord> childRecords =
                _baseDataAccessor.getChildren(path, null, options);
            record = new ZNRecordAssembler().assemble(childRecords);
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
  public <T extends HelixProperty> T getProperty(PropertyKey key)
  {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);
    ZNRecord record = null;
    try
    {
      record = _baseDataAccessor.get(path, null, options);
    }
    catch (ZkNoNodeException e)
    {
      // OK
    }

    switch (type)
    {
    case CURRENTSTATES:
      // check if bucketized
      if (record != null)
      {
        CurrentState curState = new CurrentState(record);
        int bucketSize = curState.getBucketSize();
        if (bucketSize > 0)
        {
          List<ZNRecord> childRecords =
              _baseDataAccessor.getChildren(path, null, options);
          record = new ZNRecordAssembler().assemble(childRecords);
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
  public boolean removeProperty(PropertyKey key)
  {
    PropertyType type = key.getType();
    String path = key.getPath();
    return _baseDataAccessor.remove(path);
  }

  @Override
  public List<String> getChildNames(PropertyKey key)
  {
    PropertyType type = key.getType();
    String parentPath = key.getPath();
    int options = constructOptions(type);
    return _baseDataAccessor.getChildNames(parentPath, options);
  }

  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key)
  {
    PropertyType type = key.getType();
    String parentPath = key.getPath();
    int options = constructOptions(type);
    List<T> childValues = new ArrayList<T>();

    List<ZNRecord> children = _baseDataAccessor.getChildren(parentPath, null, options);
    for (ZNRecord record : children)
    {
      switch (type)
      {
      case CURRENTSTATES:
        // check if bucketized
        if (record != null)
        {
          CurrentState curState = new CurrentState(record);
          int bucketSize = curState.getBucketSize();
          if (bucketSize > 0)
          {
            String path = parentPath + "/" + record.getId();
            List<ZNRecord> childRecords =
                _baseDataAccessor.getChildren(path, null, options);
            record = new ZNRecordAssembler().assemble(childRecords);
          }
        }
        break;
      default:
        break;
      }

      if (record != null)
      {
        @SuppressWarnings("unchecked")
        T t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
        childValues.add(t);
      }
    }
    return childValues;
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key)
  {
    PropertyType type = key.getType();
    String parentPath = key.getPath();
    int options = constructOptions(type);
    List<T> children = getChildValues(key);
    Map<String, T> childValuesMap = new HashMap<String, T>();
    for (T t : children)
    {
      childValuesMap.put(t.getRecord().getId(), t);
    }
    return childValuesMap;

  }

  @Override
  public Builder keyBuilder()
  {
    return _propertyKeyBuilder;
  }

  private int constructOptions(PropertyType type)
  {
    int options = 0;
    if (type.isPersistent())
    {
      options = options | BaseDataAccessor.Option.PERSISTENT;
    }
    else
    {
      options = options | BaseDataAccessor.Option.EPHEMERAL;
    }
    return options;
  }

  @Override
  public <T extends HelixProperty> boolean[] createChildren(List<PropertyKey> keys,
                                                            List<T> children)
  {
    // TODO: add validation
    int options = -1;
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < keys.size(); i++)
    {
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
  public <T extends HelixProperty> boolean[] setChildren(List<PropertyKey> keys,
                                                         List<T> children)
  {
    int options = -1;
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < keys.size(); i++)
    {
      PropertyKey key = keys.get(i);
      PropertyType type = key.getType();
      String path = key.getPath();
      paths.add(path);
      HelixProperty value = children.get(i);
      records.add(value.getRecord());
      options = constructOptions(type);
    }
    return _baseDataAccessor.setChildren(paths, records, options);

  }

  @Override
  public BaseDataAccessor<ZNRecord> getBaseDataAccessor()
  {
    return _baseDataAccessor;
  }

  @Override
  public <T extends HelixProperty> boolean[] updateChildren(List<String> paths,
                                                            List<DataUpdater<ZNRecord>> updaters,
                                                            int options)
  {
    return _baseDataAccessor.updateChildren(paths, updaters, options);
  }

  private String getPropertyTransferUrl()
  {
    if (_zkPropertyTransferSvcUrl == null)
    {
      refreshZkPropertyTransferUrl();
    }
    return _zkPropertyTransferSvcUrl;
  }

  public void shutdown()
  {
    _zkPropertyTransferClient.shutdown();
  }

  @Override
  public void onControllerChange(NotificationContext changeContext)
  {
    refreshZkPropertyTransferUrl();
  }

  void refreshZkPropertyTransferUrl()
  {
    LiveInstance leader = getProperty(keyBuilder().controllerLeader());
    if (leader != null)
    {
      _zkPropertyTransferSvcUrl = leader.getWebserviceUrl();
    }
    else
    {
      _zkPropertyTransferSvcUrl = null;
    }
  }
}
