package com.linkedin.clustermanager.agent.file;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.CMConstants.ChangeType;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ClusterView;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;
import com.linkedin.clustermanager.tools.ClusterViewSerializer;
import com.linkedin.clustermanager.tools.IdealStateCalculatorByShuffling;

public class FileBasedClusterManager implements ClusterManager
{
  private static final Logger LOG = Logger
      .getLogger(FileBasedClusterManager.class.getName());
  private ClusterView _clusterView; // for backward compatibility, TODO remove it later
  private final ClusterDataAccessor _fileDataAccessor;
  // private final FileBasedDataAccessor _fileDataAccessor;
  // private final String _rootNamespace = "/tmp/testFilePropertyStoreIntegration";
  
  private final String _clusterName;
  private final InstanceType _instanceType;
  // private final String _staticClusterConfigFile;
  private final String _instanceName;
  private boolean _isConnected;
  private List<CallbackHandlerForFile> _handlers;

  public static final String _sessionId = "12345";
  public static final String configFile = "configFile";
  
  public FileBasedClusterManager(String clusterName, String instanceName,
      InstanceType instanceType, String staticClusterConfigFile)
  {
    this._clusterName = clusterName;
    this._instanceName = instanceName;
    this._instanceType = instanceType;
    // this._staticClusterConfigFile = staticClusterConfigFile;

    _handlers = new ArrayList<CallbackHandlerForFile>();
    
    _fileDataAccessor = new DummyFileDataAccessor();
    this._clusterView = ClusterViewSerializer.deserialize(new File(staticClusterConfigFile));
  }

  private static Message createSimpleMessage(ZNRecord idealStateRecord,
      String stateUnitKey, String instanceName, String currentState,
      String nextState)
  {
    Message message = new Message();
    String uuid = UUID.randomUUID().toString();
    message.setId(uuid);
    message.setMsgId(uuid);
    String hostName = "UNKNOWN";
    try
    {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e)
    {
      // logger.info("Unable to get Host name. Will set it to UNKNOWN, mostly ignorable",
      // e);
      // can ignore it,
    }
    message.setSrcName(hostName);
    message.setTgtName(instanceName);
    message.setMsgState("new");
    message.setStateUnitKey(stateUnitKey);
    message.setStateUnitGroup(idealStateRecord.getId());
    message.setFromState(currentState);
    message.setToState(nextState);

    // String sessionId =
    // _liveInstanceDataHolder.getSessionId(instanceName);
    // message.setTgtSessionId(sessionId);
    message.setTgtSessionId(FileBasedClusterManager._sessionId);
    return message;
  }

  // FIXIT
  // reorder the messages to reduce the possibility that a S->M message for a given
  // db partition gets executed before a O->S message
  private static void addMessageInOrder(List<ZNRecord> msgList, Message newMsg)
  {
    String toState = newMsg.getToState();
    if (toState.equals("MASTER"))
    {
      msgList.add(newMsg);
    }
    if (toState.equals("SLAVE"))
    {
      msgList.add(0, newMsg);
    }
  }
  
  private static List<Message> computeMessagesForSimpleTransition(ZNRecord idealStateRecord)
  {
    // Map<String, List<Message>> msgListMap = new HashMap<String, List<Message>>();
    // List<Message> offlineToSlaveMsg = new ArrayList<Message>();
    // List<Message> slaveToMasterMsg = new ArrayList<Message>();
    // msgListMap.put("O->S", offlineToSlaveMsg);
    // msgListMap.put("S->M", slaveToMasterMsg);
    List<Message> msgList = new ArrayList<Message>();
    
    // messages = new ArrayList<Message>();
    IdealState idealState = new IdealState(idealStateRecord);
    for (String stateUnitKey : idealState.stateUnitSet())
    {
      Map<String, String> instanceStateMap;
      instanceStateMap = idealState.getInstanceStateMap(stateUnitKey);
      for (String instanceName : instanceStateMap.keySet())
      {
        String desiredState = idealState.get(stateUnitKey, instanceName);

        if (desiredState.equals("MASTER"))
        {
          Message message = createSimpleMessage(idealStateRecord, stateUnitKey,
              instanceName, "OFFLINE", "SLAVE");
          msgList.add(message);
          message = createSimpleMessage(idealStateRecord, stateUnitKey,
              instanceName, "SLAVE", "MASTER");
          msgList.add(message);
        } else
        {
          Message message = createSimpleMessage(idealStateRecord, stateUnitKey,
              instanceName, "OFFLINE", "SLAVE");
          msgList.add(message);
        }

      }
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
                                                            List<DBParam> dbParams, 
                                                            int replica)
  {
    // create mock cluster view
    ClusterView view = new ClusterView();

    // add nodes
    List<ZNRecord> nodeConfigList = new ArrayList<ZNRecord>();
    List<String> instanceNames = new ArrayList<String>();

    Arrays.sort(nodesInfo, new Comparator<String>() {

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
      String host = nodeInfo.substring(0, lastPos);
      String port = nodeInfo.substring(lastPos + 1);
      ZNRecord nodeConfig = new ZNRecord();

      String nodeId = host + "_" + port;
      nodeConfig.setId(nodeId);
      nodeConfig.setSimpleField(ClusterDataAccessor.InstanceConfigProperty.ENABLED.toString(), 
                                  Boolean.toString(true));
      nodeConfig.setSimpleField(ClusterDataAccessor.InstanceConfigProperty.HOST.toString(), 
                                  host);
      nodeConfig.setSimpleField(ClusterDataAccessor.InstanceConfigProperty.PORT.toString(), 
                                  port);
      
      instanceNames.add(nodeId);

      nodeConfigList.add(nodeConfig);
    }
    view.setClusterPropertyList(ClusterPropertyType.CONFIGS, nodeConfigList);

    // set IDEALSTATES
    // compute ideal states for each db
    List<ZNRecord> idealStates = new ArrayList<ZNRecord>();
    for (DBParam dbParam : dbParams)
    {
      ZNRecord result = IdealStateCalculatorByShuffling.calculateIdealState(instanceNames, 
                                                                            dbParam.partitions, 
                                                                            replica, 
                                                                            dbParam.name);

      idealStates.add(result);
    }
    view.setClusterPropertyList(ClusterPropertyType.IDEALSTATES, idealStates);

    // calculate messages for transition using naive algorithm
    Map<String, List<ZNRecord>> msgListForInstance = new HashMap<String, List<ZNRecord>>();
    List<ZNRecord> idealStatesArray = view.getClusterPropertyList(ClusterPropertyType.IDEALSTATES);
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
    for (Map.Entry< String, List<ZNRecord> > entry : msgListForInstance.entrySet())
    {
      String instance = entry.getKey();
      List<ZNRecord> msgList = entry.getValue();

      ClusterView.MemberInstance ins = view.getMemberInstance(instance, true);
      ins.setInstanceProperty(InstancePropertyType.MESSAGES, msgList);
      // ins.setInstanceProperty(InstancePropertyType.CURRENTSTATES, null);
      // ins.setInstanceProperty(InstancePropertyType.ERRORS, null);
      // ins.setInstanceProperty(InstancePropertyType.STATUSUPDATES, null);
      insList.add(ins);
    }

    // sort it
    ClusterView.MemberInstance[] insArray = new ClusterView.MemberInstance[insList.size()]; 
    insArray = insList.toArray(insArray);
    Arrays.sort(insArray, new Comparator<ClusterView.MemberInstance>() {

      @Override
      public int compare(ClusterView.MemberInstance ins1, ClusterView.MemberInstance ins2)
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
    listener.onIdealStateChange(this._clusterView
        .getClusterPropertyList(ClusterPropertyType.IDEALSTATES), context);

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
        .getInstanceProperty(InstancePropertyType.MESSAGES);
    listener.onMessage(instanceName, messages, context);
  }

  @Override
  public void addCurrentStateChangeListener(
      CurrentStateChangeListener listener, String instanceName, String sessionId)
  {
    throw new UnsupportedOperationException(
        "addCurrentStateChangeListener is not supported by File Based cluster manager");  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener)
  {
    throw new UnsupportedOperationException(
        "addExternalViewChangeListener() is NOT supported by File Based cluster manager");
  }

  @Override
  public ClusterDataAccessor getDataAccessor()
  {
    return _fileDataAccessor;
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

  private static Options constructCommandLineOptions()
  {
    Option fileOption = OptionBuilder.withLongOpt(configFile)
        .withDescription("Provide file to write states/messages").create();
    fileOption.setArgs(1);
    fileOption.setRequired(true);
    fileOption.setArgName("File to read states/messages (Required)");

    Options options = new Options();
    options.addOption(fileOption);
    return options;

  }

  public static CommandLine processCommandLineArgs(String[] cliArgs)
      throws Exception
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    // CommandLine cmd = null;

    try
    {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe)
    {
      System.err
          .println("CommandLineClient: failed to parse command-line options: "
              + pe.toString());
      // printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  public static ClusterView convertStateModelMapToClusterView(String outFile,
      String instanceName, StateModelFactory<StateModel> stateModelFactory)
  {
    Map<String, StateModel> currentStateMap = stateModelFactory.getStateModelMap();
    ClusterView curView = new ClusterView();
    
    ClusterView.MemberInstance memberInstance = curView.getMemberInstance(instanceName, true);
    List<ZNRecord> curStateList = new ArrayList<ZNRecord>();
    
    for (Map.Entry<String, StateModel> entry : currentStateMap.entrySet())
    {
      String stateUnitKey = entry.getKey();
      String curState = entry.getValue().getCurrentState();
      ZNRecord record = new ZNRecord();
      record.setId(stateUnitKey);
      record.simpleFields.put(stateUnitKey, curState);
      curStateList.add(record);
    }
    
    memberInstance.setInstanceProperty(ClusterDataAccessor.InstancePropertyType.CURRENTSTATES, curStateList);
    
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
  
  public static boolean VerifyFileBasedClusterStates(String instanceName, String expectedFile, String curFile)
  {
    boolean ret = true;
    ClusterView expectedView = ClusterViewSerializer.deserialize(new File(expectedFile)); // readClusterView(expectedFile);
    ClusterView curView = ClusterViewSerializer.deserialize(new File(curFile)); // readClusterView(curFile);
    
    int nonOfflineNr = 0;

    // ideal_state for instance with the given instanceName
    Map<String, String> idealStates = new HashMap<String, String>();
    for (ZNRecord idealStateItem : expectedView.getClusterPropertyList(ClusterPropertyType.IDEALSTATES))
    {
      Map<String, Map<String, String>> allIdealStates = idealStateItem.getMapFields();

      for (Map.Entry<String, Map<String, String>> entry : allIdealStates.entrySet())
      {
        if (entry.getValue().containsKey(instanceName))
        {
          String state = entry.getValue().get(instanceName);
          idealStates.put(entry.getKey(), state);
        }
      }
    }

    ClusterView.MemberInstance memberInstance = curView.getMemberInstance(instanceName, false);
    List<ZNRecord> curStateList = memberInstance.getInstanceProperty(ClusterDataAccessor.InstancePropertyType.CURRENTSTATES);

    if (curStateList.size() != idealStates.size())
    {
      LOG.error("Number of current states (" + curStateList.size()
                       + ") mismatch " + "number of ideal states ("
                       + idealStates.size() + ")");
      return false;
    }

    for (ZNRecord record : curStateList)
    {
      String stateUnitKey = record.id;
      String curState = record.simpleFields.get(stateUnitKey);
      
      if (!curState.equalsIgnoreCase("offline"))
        nonOfflineNr++;
      
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

  private void addLiveInstance()
  { 
      ZNRecord metaData = new ZNRecord();
      // set it from the session
      metaData.setId(_instanceName);
      metaData.setSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString(),
          _sessionId);
      _fileDataAccessor.setClusterProperty(ClusterPropertyType.LIVEINSTANCES,
          _instanceName, metaData, CreateMode.EPHEMERAL);
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
  public boolean removeListener(Object listener) {
  	// TODO Auto-generated method stub
  	return false;
  }
  
  
  private CallbackHandlerForFile createCallBackHandler(String path, Object listener,
      EventType[] eventTypes, ChangeType changeType)
  {
    if (listener == null)
    {
      throw new ClusterManagerException("Listener cannot be null");
    }
    return new CallbackHandlerForFile(this, path, listener, eventTypes, changeType);
  }

  
  // TODO remove it
  // temp test
  public static void main(String[] args) throws Exception
  {
    // for temporary test only
    System.out.println("Generate static config file for cluster");
    String file = "/tmp/clusterView.json";

    // CommandLine cmd = processCommandLineArgs(args);
    // file = cmd.getOptionValue(configFile);

    // create fake db names & nodes info
    List<FileBasedClusterManager.DBParam> dbParams = new ArrayList<FileBasedClusterManager.DBParam>();
    // dbParams.add(new FileBasedClusterManager.DBParam("BizFollow", 1));
    // dbParams.add(new FileBasedClusterManager.DBParam("BizProfile", 1));
    dbParams.add(new FileBasedClusterManager.DBParam("EspressoDB", 10));
    // dbParams.add(new FileBasedClusterManager.DBParam("MailboxDB", 128));
    dbParams.add(new FileBasedClusterManager.DBParam("MyDB", 8));
    dbParams.add(new FileBasedClusterManager.DBParam("schemata", 1));

    String[] nodesInfo = { "localhost:8900", 
                           "localhost:8901" }; 
                           // "localhost:8902", 
                           // "localhost:8903", 
                           // "localhost:8904" };

    int replica = 0;
    
    // ClusterViewSerializer serializer = new ClusterViewSerializer(file);
    ClusterView view = generateStaticConfigClusterView(nodesInfo, dbParams, replica);
    

    ClusterViewSerializer.serialize(view, new File(file));
    // System.out.println(new String(bytes));

    ClusterView restoredView = ClusterViewSerializer.deserialize(new File(file));
    // System.out.println(restoredView);

    byte[] bytes = ClusterViewSerializer.serialize(restoredView);
    System.out.println(new String(bytes));

  }

  @Override
  public ClusterManagementService getClusterManagmentTool()
  {
    // TODO Auto-generated method stub
    return null;
  }

}
