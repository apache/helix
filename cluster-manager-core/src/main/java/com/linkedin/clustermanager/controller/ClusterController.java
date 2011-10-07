package com.linkedin.clustermanager.controller;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.StateModelDefinition;

/**
 * Cluster Controllers main goal is to keep the cluster state as close as
 * possible to Ideal State. It does this by listening to changes in cluster
 * state and scheduling new tasks to get cluster state to best possible ideal
 * state. Every instance of this class can control can control only one cluster
 */
//Use GenericClusterController instead
@Deprecated
public class ClusterController implements ConfigChangeListener,
    IdealStateChangeListener, LiveInstanceChangeListener, MessageListener,
    CurrentStateChangeListener, ExternalViewChangeListener
{
  private static Logger logger = Logger.getLogger(ClusterController.class);
  private final BestPossibleIdealStateCalculator _bestPossibleIdealStateCalculator;
  private final TransitionMessageGenerator _transitionMessageGenerator;

  private StateModelDefinition _stateModelDefinition;
  private final CurrentStateHolder _currentStateHolder;
  private final LiveInstanceDataHolder _liveInstanceDataHolder;
  private final MessageHolder _messageHolder;
  private final Set<String> _instanceSubscriptionList;
  private final ExternalViewGenerator _routingInfoProvider;
  private final InstanceConfigHolder _instanceConfigHolder;

  public ClusterController()
  {
    this(new StorageStateModelDefinition(new ZNRecord("StorageStateModel")));
  }

  public ClusterController(StateModelDefinition stateModelDefinition)
  {

    _currentStateHolder = new CurrentStateHolder();
    _stateModelDefinition = stateModelDefinition;
    _instanceSubscriptionList = new HashSet<String>();
    _routingInfoProvider = new ExternalViewGenerator();
    _messageHolder = new MessageHolder();
    _liveInstanceDataHolder = new LiveInstanceDataHolder();
    _bestPossibleIdealStateCalculator = new BestPossibleIdealStateCalculator(
        _liveInstanceDataHolder);
    _instanceConfigHolder = new InstanceConfigHolder();
    _transitionMessageGenerator = new TransitionMessageGenerator(
        _stateModelDefinition, _currentStateHolder, _messageHolder,
        _liveInstanceDataHolder, _instanceConfigHolder);
  }

  public StateModelDefinition getStateModelDefinition()
  {
    return _stateModelDefinition;
  }

  public void setStateModelDefinition(StateModelDefinition _stateModelDefinition)
  {
    this._stateModelDefinition = _stateModelDefinition;
  }

  @Override
  public void onMessage(String instanceName, List<ZNRecord> messages,
      NotificationContext changeContext)
  {
    logger.info("START:ClusterController.onMessage()");
    _messageHolder.update(instanceName, messages);
    logger.info("END:ClusterController.onMessage()");
  }

  @Override
  public void onLiveInstanceChange(List<ZNRecord> liveInstances,
      NotificationContext changeContext)
  {
    if (liveInstances == null)
    {
      liveInstances = Collections.emptyList();
    }
    logger.info("START: ClusterController.onLiveInstanceChange()");
    _liveInstanceDataHolder.refresh(liveInstances);
    for (ZNRecord instance : liveInstances)
    {
      String instanceName = instance.getId();
      String clientSessionId  = instance.getSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString());
      if (!_instanceSubscriptionList.contains(instanceName))
      {
        try
        {
          changeContext.getManager().addCurrentStateChangeListener(this,
              instanceName, clientSessionId);
          changeContext.getManager().addMessageListener(this, instanceName);
        } catch (Exception e)
        {
          logger.error(
              "Exception adding current state and message listener for instance:"
                  + instanceName, e);
        }

        _instanceSubscriptionList.add(instanceName);
      }
    }

    if (NotificationContext.Type.CALLBACK.equals(changeContext.getType()))
    {
      runClusterRebalanceAlgo(changeContext);
    } else if (NotificationContext.Type.INIT.equals(changeContext.getType()))
    {
    }
    logger.info("END: ClusterController.onLiveInstanceChange()");
  }

  @Override
  public void onExternalViewChange(List<ZNRecord> externalView,
      NotificationContext changeContext)
  {
    logger.info("ClusterController.onExternalViewChange()");
  }

  @Override
  public void onConfigChange(List<ZNRecord> configs,
      NotificationContext changeContext)
  {
    if (configs == null)
    {
      configs = Collections.emptyList();
    }
    _instanceConfigHolder.refresh(configs);
    runClusterRebalanceAlgo(changeContext);
    logger.info("ClusterController.onConfigChange()");
  }

  @Override
  public void onIdealStateChange(List<ZNRecord> idealStates,
      NotificationContext changeContext)
  {
    logger.info("START: ClusterController.onIdealStateChange()");
    runClusterRebalanceAlgo(changeContext);
    logger.info("END: ClusterController.onIdealStateChange()");
  }

  /**
   * Generic method which will run when ever there is a change in following -
   * IdealState - Current State change - liveInstanceChange
   * 
   * @param changeContext
   */
  private void runClusterRebalanceAlgo(NotificationContext changeContext)
  {
    logger.info("START:ClusterController.runClusterRebalanceAlgo()");
    ClusterDataAccessor client = changeContext.getManager().getDataAccessor();
    List<ZNRecord> idealStates;
    idealStates = client
        .getClusterPropertyList(ClusterPropertyType.IDEALSTATES);
    refreshData(changeContext);
    for (ZNRecord idealStateRecord : idealStates)
    {
      IdealState idealState = new IdealState(idealStateRecord);
      IdealState bestPossibleIdealState;
      bestPossibleIdealState = _bestPossibleIdealStateCalculator.compute(
          idealState, changeContext);
      List<Message> messages;
      messages = _transitionMessageGenerator.computeMessagesForTransition(
          idealState, bestPossibleIdealState, idealStateRecord);
      logger.info(messages.size() + "Messages to send");
      int i = 0;
      for (Message message : messages)
      {
        logger.info(i++ + ": Sending message to " + message.getTgtName()
            + " transition " + message.getStateUnitKey() + " from:"
            + message.getFromState() + " to:" + message.getToState());
        client.setInstanceProperty(message.getTgtName(),
            InstancePropertyType.MESSAGES, message.getId(), message.getRecord());
      }
    }
    logger.info("END:ClusterController.runClusterRebalanceAlgo()");
  }

  private boolean refreshData(NotificationContext changeContext)
  {
    ClusterDataAccessor client = changeContext.getManager().getDataAccessor();
    List<ZNRecord> liveInstances = client
        .getClusterPropertyList(ClusterPropertyType.LIVEINSTANCES);
    _liveInstanceDataHolder.refresh(liveInstances);
    for (ZNRecord record : liveInstances)
    {
      String instanceName = record.getId();
      String sessionId = record
          .getSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString());
      List<ZNRecord> messages = client.getInstancePropertyList(instanceName,
          InstancePropertyType.MESSAGES);
      Iterator<ZNRecord> msgIterator = messages.iterator();
      while (msgIterator.hasNext())
      {
        Message message = new Message(msgIterator.next());
        String tgtSessionId = message.getTgtSessionId();
        if (tgtSessionId == null || !tgtSessionId.equals(sessionId))
        {
          logger.info("Removing old message:" + message.getId());
          client.removeInstanceProperty(instanceName,
              InstancePropertyType.MESSAGES, message.getId());
          msgIterator.remove();
        }
      }
      _messageHolder.update(instanceName, messages);
      List<ZNRecord> currentStates = client.getInstancePropertyList(
          instanceName, changeContext.getManager().getSessionId(), InstancePropertyType.CURRENTSTATES);
      _currentStateHolder.refresh(instanceName, currentStates);
    }
    return false;
  }

  @Override
  public void onStateChange(String instanceName, List<ZNRecord> statesInfo,
      NotificationContext changeContext)
  {
    logger.info("START:ClusterController.onStateChange()");
    _currentStateHolder.refresh(instanceName, statesInfo);
    // We want to run algo only when there is change in current state in ZK
    if (NotificationContext.Type.CALLBACK.equals(changeContext.getType()))
    {

      runClusterRebalanceAlgo(changeContext);
    } else if (NotificationContext.Type.INIT.equals(changeContext.getType()))
    {
      // This will be called during the initialization of listener when a
      // node comes up. The cluster algo will be invoked from
      // onLiveInstanceChange method
      // Will try to remove this checking later to keep the logic
      // independent of NotificationType which is the case for most
      // callbacks
    }
    ClusterDataAccessor client = changeContext.getManager().getDataAccessor();
    List<ZNRecord> idealStates = client
        .getClusterPropertyList(ClusterPropertyType.IDEALSTATES);
    List<ZNRecord> externalViewList;
    Map<String, List<ZNRecord>> currentStatesListMap = _currentStateHolder
        .getCurrentStatesListMap();
    externalViewList = _routingInfoProvider.computeExternalView(
        currentStatesListMap, idealStates);
    for (ZNRecord subView : externalViewList)
    {
      client.setClusterProperty(ClusterPropertyType.EXTERNALVIEW,
          subView.getId(), subView);
    }
    logger.info("END:ClusterController.onStateChange()");
  }

}
