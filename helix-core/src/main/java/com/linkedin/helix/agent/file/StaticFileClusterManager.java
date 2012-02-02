package com.linkedin.helix.agent.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.ClusterManagementService;
import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.ClusterView;
import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;
import com.linkedin.helix.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig.InstanceConfigProperty;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.StateMachEngine;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.tools.ClusterViewSerializer;
import com.linkedin.helix.tools.IdealStateCalculatorByShuffling;

public class StaticFileClusterManager implements ClusterManager
{
  private static final Logger LOG = Logger
      .getLogger(StaticFileClusterManager.class.getName());
  // for backward compatibility
  // TODO remove it later
  private final ClusterView _clusterView;
  private final String _clusterName;
  private final InstanceType _instanceType;
  private final String _instanceName;
  private boolean _isConnected;
  public static final String _sessionId = "12345";
  public static final String configFile = "configFile";

  public StaticFileClusterManager(String clusterName, String instanceName,
      InstanceType instanceType, String clusterViewFile)
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _instanceType = instanceType;
    _clusterView = ClusterViewSerializer
        .deserialize(new File(clusterViewFile));
  }

  // FIXIT
  // reorder the messages to reduce the possibility that a S->M message for a
  // given
  // db partition gets executed before a O->S message
  private static void addMessageInOrder(List<ZNRecord> msgList, Message newMsg)
  {
    String toState = newMsg.getToState();
    if (toState.equals("MASTER"))
    {
      msgList.add(newMsg.getRecord());
    }
    if (toState.equals("SLAVE"))
    {
      msgList.add(0, newMsg.getRecord());
    }
  }

  private static List<Message> computeMessagesForSimpleTransition(
      ZNRecord idealStateRecord)
  {
    List<Message> msgList = new ArrayList<Message>();

    IdealState idealState = new IdealState(idealStateRecord);
    for (String stateUnitKey : idealState.getResourceKeySet())
    {
      Map<String, String> instanceStateMap;
      instanceStateMap = idealState.getInstanceStateMap(stateUnitKey);
    }

    return msgList;
  }

  public static class DBParam
  {
    public String name;
    public int partitions;

    public DBParam(String n, int p)
    {
      name = n;
      partitions = p;
    }
  }

  public static ClusterView generateStaticConfigClusterView(String[] nodesInfo,
      List<DBParam> dbParams, int replica)
  {
    // create mock cluster view
    ClusterView view = new ClusterView();

    // add nodes
    List<ZNRecord> nodeConfigList = new ArrayList<ZNRecord>();
    List<String> instanceNames = new ArrayList<String>();

    Arrays.sort(nodesInfo, new Comparator<String>()
    {

      @Override
      public int compare(String str1, String str2)
      {
        return str1.compareTo(str2);
      }

    });

    // set CONFIGS
    for (String nodeInfo : nodesInfo)
    {
      int lastPos = nodeInfo.lastIndexOf(":");
      if (lastPos == -1)
      {
        throw new IllegalArgumentException("nodeInfo should be in format of host:port, " + nodeInfo);
      }

      String host = nodeInfo.substring(0, lastPos);
      String port = nodeInfo.substring(lastPos + 1);
      String nodeId = host + "_" + port;
      ZNRecord nodeConfig = new ZNRecord(nodeId);

      nodeConfig.setSimpleField(InstanceConfigProperty.ENABLED.toString(), Boolean.toString(true));
      nodeConfig.setSimpleField(InstanceConfigProperty.HOST.toString(), host);
      nodeConfig.setSimpleField(InstanceConfigProperty.PORT.toString(), port);

      instanceNames.add(nodeId);

      nodeConfigList.add(nodeConfig);
    }
    view.setClusterPropertyList(PropertyType.CONFIGS, nodeConfigList);

    // set IDEALSTATES
    // compute ideal states for each db
    List<ZNRecord> idealStates = new ArrayList<ZNRecord>();
    for (DBParam dbParam : dbParams)
    {
      ZNRecord result = IdealStateCalculatorByShuffling.calculateIdealState(
          instanceNames, dbParam.partitions, replica, dbParam.name);

      idealStates.add(result);
    }
    view.setClusterPropertyList(PropertyType.IDEALSTATES, idealStates);

    // calculate messages for transition using naive algorithm
    Map<String, List<ZNRecord>> msgListForInstance = new HashMap<String, List<ZNRecord>>();
    List<ZNRecord> idealStatesArray = view
        .getPropertyList(PropertyType.IDEALSTATES);
    for (ZNRecord idealStateRecord : idealStatesArray)
    {
      // IdealState idealState = new IdealState(idealStateRecord);

      List<Message> messages = computeMessagesForSimpleTransition(idealStateRecord);

      for (Message message : messages)
      {
        // logger.info("Sending message to " + message.getTgtName() +
        // " transition "
        // + message.getStateUnitKey() + " from:" +
        // message.getFromState() +
        // " to:"
        // + message.getToState());
        // client.addMessage(message, message.getTgtName());
        String instance = message.getTgtName();
        List<ZNRecord> msgList = msgListForInstance.get(instance);
        if (msgList == null)
        {
          msgList = new ArrayList<ZNRecord>();
          msgListForInstance.put(instance, msgList);
        }
        // msgList.add(message);
        addMessageInOrder(msgList, message);
      }
    }

    // set INSTANCES
    // put message lists into cluster view
    List<ClusterView.MemberInstance> insList = new ArrayList<ClusterView.MemberInstance>();
    for (Map.Entry<String, List<ZNRecord>> entry : msgListForInstance
        .entrySet())
    {
      String instance = entry.getKey();
      List<ZNRecord> msgList = entry.getValue();

      ClusterView.MemberInstance ins = view.getMemberInstance(instance, true);
      ins.setInstanceProperty(PropertyType.MESSAGES, msgList);
      // ins.setInstanceProperty(InstancePropertyType.CURRENTSTATES, null);
      // ins.setInstanceProperty(InstancePropertyType.ERRORS, null);
      // ins.setInstanceProperty(InstancePropertyType.STATUSUPDATES, null);
      insList.add(ins);
    }

    // sort it
    ClusterView.MemberInstance[] insArray = new ClusterView.MemberInstance[insList
        .size()];
    insArray = insList.toArray(insArray);
    Arrays.sort(insArray, new Comparator<ClusterView.MemberInstance>()
    {

      @Override
      public int compare(ClusterView.MemberInstance ins1,
          ClusterView.MemberInstance ins2)
      {
        return ins1.getInstanceName().compareTo(ins2.getInstanceName());
      }

    });

    insList = Arrays.asList(insArray);
    view.setInstances(insList);

    return view;
  }

  @Override
  public void disconnect()
  {
    _isConnected = false;
  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener)
  {

    NotificationContext context = new NotificationContext(this);
    context.setType(NotificationContext.Type.INIT);
    List<ZNRecord> idealStates = _clusterView.getPropertyList(PropertyType.IDEALSTATES);
    listener.onIdealStateChange(ZNRecordDecorator.convertToTypedList(IdealState.class, idealStates),
                                context);
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener)
  {
    throw new UnsupportedOperationException(
        "addLiveInstanceChangeListener is not supported by File Based cluster manager");
  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener)
  {
    throw new UnsupportedOperationException(
        "addConfigChangeListener() is NOT supported by File Based cluster manager");
  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName)
  {
    NotificationContext context = new NotificationContext(this);
    context.setType(NotificationContext.Type.INIT);
    List<ZNRecord> messages;
    messages = _clusterView.getMemberInstance(instanceName, true)
        .getInstanceProperty(PropertyType.MESSAGES);
    listener.onMessage(instanceName, ZNRecordDecorator.convertToTypedList(Message.class, messages),
                       context);
  }

  @Override
  public void addCurrentStateChangeListener(
      CurrentStateChangeListener listener, String instanceName, String sessionId)
  {
    throw new UnsupportedOperationException(
        "addCurrentStateChangeListener is not supported by File Based cluster manager");
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener)
  {
    throw new UnsupportedOperationException(
        "addExternalViewChangeListener() is NOT supported by File Based cluster manager");
  }

  @Override
  public ClusterDataAccessor getDataAccessor()
  {
    return null;
  }

  @Override
  public String getClusterName()
  {
    return _clusterName;
  }

  @Override
  public String getInstanceName()
  {
    return _instanceName;
  }

  @Override
  public void connect()
  {
    _isConnected = true;
  }

  @Override
  public String getSessionId()
  {
    return _sessionId;
  }

  public static ClusterView convertStateModelMapToClusterView(String outFile,
      String instanceName, StateModelFactory<StateModel> stateModelFactory)
  {
    Map<String, StateModel> currentStateMap = stateModelFactory
        .getStateModelMap();
    ClusterView curView = new ClusterView();

    ClusterView.MemberInstance memberInstance = curView.getMemberInstance(
        instanceName, true);
    List<ZNRecord> curStateList = new ArrayList<ZNRecord>();

    for (Map.Entry<String, StateModel> entry : currentStateMap.entrySet())
    {
      String stateUnitKey = entry.getKey();
      String curState = entry.getValue().getCurrentState();
      ZNRecord record = new ZNRecord(stateUnitKey);
      record.setSimpleField(stateUnitKey, curState);
      curStateList.add(record);
    }

    memberInstance.setInstanceProperty(
        PropertyType.CURRENTSTATES, curStateList);

    // serialize to file
    // String outFile = "/tmp/curClusterView_" + instanceName +".json";
    if (outFile != null)
    {
      // ClusterViewSerializer serializer = new ClusterViewSerializer(outFile);
      // serializer.serialize(curView);
      ClusterViewSerializer.serialize(curView, new File(outFile));
    }

    return curView;
  }

  public static boolean verifyFileBasedClusterStates(String instanceName,
      String expectedFile, String curFile)
  {
    boolean ret = true;
    ClusterView expectedView = ClusterViewSerializer.deserialize(new File(
        expectedFile));
    ClusterView curView = ClusterViewSerializer.deserialize(new File(curFile));

    // ideal_state for instance with the given instanceName
    Map<String, String> idealStates = new HashMap<String, String>();
    for (ZNRecord idealStateItem : expectedView
        .getPropertyList(PropertyType.IDEALSTATES))
    {
      Map<String, Map<String, String>> allIdealStates = idealStateItem
          .getMapFields();

      for (Map.Entry<String, Map<String, String>> entry : allIdealStates
          .entrySet())
      {
        if (entry.getValue().containsKey(instanceName))
        {
          String state = entry.getValue().get(instanceName);
          idealStates.put(entry.getKey(), state);
        }
      }
    }

    ClusterView.MemberInstance memberInstance = curView.getMemberInstance(
        instanceName, false);
    List<ZNRecord> curStateList = memberInstance
        .getInstanceProperty(PropertyType.CURRENTSTATES);

    if (curStateList == null && idealStates.size() > 0)
    {
      LOG.info("current state is null");
      return false;
    } else if (curStateList == null && idealStates.size() == 0)
    {
      LOG.info("empty current state and ideal state");
      return true;
    } else if (curStateList.size() != idealStates.size())
    {
      LOG.info("Number of current states (" + curStateList.size()
          + ") mismatch " + "number of ideal states (" + idealStates.size()
          + ")");
      return false;
    }

    for (ZNRecord record : curStateList)
    {
      String stateUnitKey = record.getId();
      String curState = record.getSimpleField(stateUnitKey);

      // if (!curState.equalsIgnoreCase("offline"))
      // nonOfflineNr++;

      if (!idealStates.containsKey(stateUnitKey))
      {
        LOG.error("Current state does not contain " + stateUnitKey);
        ret = false;
        continue;
      }

      String idealState = idealStates.get(stateUnitKey);
      if (!curState.equalsIgnoreCase(idealState))
      {
        LOG.error("State mismatch--unit_key:" + stateUnitKey + " cur:"
            + curState + " ideal:" + idealState + " instance_name:"
            + instanceName);
        ret = false;
        continue;
      }

    }

    return ret;
  }

  @Override
  public boolean isConnected()
  {
    return _isConnected;
  }

  @Override
  public long getLastNotificationTime()
  {
    return 0;
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener)
  {
    throw new UnsupportedOperationException(
        "addControllerListener() is NOT supported by File Based cluster manager");
  }

  @Override
  public boolean removeListener(Object listener)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ClusterManagementService getClusterManagmentTool()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PropertyStore<ZNRecord> getPropertyStore()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClusterMessagingService getMessagingService()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InstanceType getInstanceType()
  {
    return _instanceType;
  }

  @Override
  public void addHealthStateChangeListener(HealthStateChangeListener listener,
  		String instanceName) throws Exception {
  	// TODO Auto-generated method stub
  
  }

  @Override
  public String getVersion()
  {
    throw new UnsupportedOperationException("getVersion() not implemented in FileClusterManager");
  }

  @Override
  public StateMachEngine getStateMachineEngine()
  {
    // TODO Auto-generated method stub
    return null;
  }
}
