package com.linkedin.clustermanager.agent.zk;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.CMConstants.ChangeType;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterView.MemberInstance;
import com.linkedin.clustermanager.util.CMUtil;

import static com.linkedin.clustermanager.CMConstants.ChangeType.*;

public class CallbackHandler implements IZkChildListener, IZkDataListener

{

  private static Logger logger = Logger.getLogger(CallbackHandler.class);

  private final String _path;
  private final Object _listener;
  private final EventType[] _eventTypes;
  private final ClusterDataAccessor _accessor;
  private final ChangeType _changeType;
  private final ZkClient _zkClient;

  private final ClusterManager _manager;

  public CallbackHandler(ClusterManager manager, ZkClient client, String path,
      Object listener, EventType[] eventTypes, ChangeType changeType)
  {
    this._manager = manager;
    this._accessor = manager.getDataAccessor();
    this._zkClient = client;
    this._path = path;
    this._listener = listener;
    this._eventTypes = eventTypes;
    this._changeType = changeType;
    init();
  }

  public Object getPath()
  {
    return _path;
  }

  public void invoke(NotificationContext changeContext) throws Exception
  {
    // This allows the listener to work with
    synchronized (_listener)
    {
      if (logger.isDebugEnabled())
      {
        logger.debug(Thread.currentThread().getId() + " START:INVOKE "
            + changeContext.getPathChanged() + " listener:"
            + _listener.getClass().getCanonicalName());
      }
      if (_changeType == IDEAL_STATE)
      {

        IdealStateChangeListener idealStateChangeListener = (IdealStateChangeListener) _listener;
        List<ZNRecord> idealStates = getChildren(_path, true, true);
        _accessor.getClusterView().setClusterPropertyList(
            ClusterPropertyType.IDEALSTATES, idealStates);
        idealStateChangeListener.onIdealStateChange(idealStates, changeContext);

      } else if (_changeType == CONFIG)
      {

        ConfigChangeListener configChangeListener = (ConfigChangeListener) _listener;
        List<ZNRecord> configs = getChildren(_path, true, true);
        _accessor.getClusterView().setClusterPropertyList(
            ClusterPropertyType.CONFIGS, configs);
        configChangeListener.onConfigChange(configs, changeContext);

      } else if (_changeType == LIVE_INSTANCE)
      {
        LiveInstanceChangeListener liveInstanceChangeListener = (LiveInstanceChangeListener) _listener;
        List<ZNRecord> liveInstances = getChildren(_path, true, false);
        _accessor.getClusterView().setClusterPropertyList(
            ClusterPropertyType.LIVEINSTANCES, liveInstances);
        liveInstanceChangeListener.onLiveInstanceChange(liveInstances,
            changeContext);

      } else if (_changeType == CURRENT_STATE)
      {
        CurrentStateChangeListener currentStateChangeListener;
        currentStateChangeListener = (CurrentStateChangeListener) _listener;
        List<ZNRecord> currentStates = getChildren(_path, true, true);
        String instanceName = CMUtil.getInstanceNameFromPath(_path);
        MemberInstance instance;
        instance = _accessor.getClusterView().getMemberInstance(instanceName,
            true);
        instance.setInstanceProperty(InstancePropertyType.CURRENTSTATES,
            currentStates);
        currentStateChangeListener.onStateChange(instanceName, currentStates,
            changeContext);

      } else if (_changeType == MESSAGE)
      {
        MessageListener messageListener = (MessageListener) _listener;
        List<ZNRecord> messages = getChildren(_path, true, false);
        String instanceName = CMUtil.getInstanceNameFromPath(_path);
        MemberInstance memberInstance;
        memberInstance = _accessor.getClusterView().getMemberInstance(
            instanceName, true);
        memberInstance.setInstanceProperty(InstancePropertyType.MESSAGES,
            messages);
        messageListener.onMessage(instanceName, messages, changeContext);

      } else if (_changeType == EXTERNAL_VIEW)
      {
        ExternalViewChangeListener externalViewListener = (ExternalViewChangeListener) _listener;
        List<ZNRecord> externalViewList = getChildren(_path, true, true);
        externalViewListener.onExternalViewChange(externalViewList,
            changeContext);
      }
      if (logger.isDebugEnabled())
      {
        logger.debug(Thread.currentThread().getId() + " END:INVOKE "
            + changeContext.getPathChanged() + " listener:"
            + _listener.getClass().getCanonicalName());
      }
    }
  }

  private List<ZNRecord> getChildren(String path, boolean watchParent,
      boolean watchChild)
  {
    // parent watch will be set by zkClient
    List<String> children = _zkClient.getChildren(path);
    List<ZNRecord> childRecords = new ArrayList<ZNRecord>();
    for (String child : children)
    {
      String childPath = path + "/" + child;
      if (watchChild)
      {
        // its ok to subscribe changes multiple times since zkclient
        // checks the existence
        _zkClient.subscribeDataChanges(childPath, this);
      }
      ZNRecord record = _zkClient.readData(childPath, true);
      if (record != null)
      {
        childRecords.add(record);
      }

    }
    return childRecords;
  }

  public EventType[] getEventTypes()
  {
    return _eventTypes;
  }

  // this will invoke the listener so that it sets up the initial values from
  // the zookeeper if any exists
  public void init()
  {
    try
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.INIT);
      invoke(changeContext);
    } catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception
  {
    if (dataPath != null && dataPath.startsWith(_path))
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      invoke(changeContext);
    }
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception
  {
    if (dataPath != null && dataPath.startsWith(_path))
    {
      // TODO need to unsubscribe after a node is deleted
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      _zkClient.unsubscribeChildChanges(dataPath, this);
      invoke(changeContext);
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds)
      throws Exception
  {
    if (parentPath != null && parentPath.startsWith(_path))
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      invoke(changeContext);
    }
  }

}
