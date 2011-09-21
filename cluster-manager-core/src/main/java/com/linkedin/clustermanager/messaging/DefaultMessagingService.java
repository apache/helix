package com.linkedin.clustermanager.messaging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.AsyncCallback;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.Criteria;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;

public class DefaultMessagingService implements ClusterMessagingService
{
  private final ClusterManager _manager;
  private CriteriaEvaluator _evaluator;

  public DefaultMessagingService(ClusterManager manager)
  {
    _evaluator = new CriteriaEvaluator();
    _manager = manager;
  }

  @Override
  public int send(Criteria recipientCriteria, Message message)
  {
    ClusterDataAccessor dataAccessor = _manager.getDataAccessor();
    InstanceType instanceType = recipientCriteria.getRecipientInstanceType();
    String resourceGroup = recipientCriteria.getResourceGroup();
    if (instanceType == InstanceType.CONTROLLER)
    {
      message.setSrcName(_manager.getInstanceName());
      dataAccessor.setControllerProperty(ControllerPropertyType.MESSAGES,
          message.getRecord(), CreateMode.PERSISTENT);
    } else if (instanceType == InstanceType.PARTICIPANT)
    {
      //todo:optimize and read only resource groups needed
      List<Map<String,String>> rows = new ArrayList<Map<String,String>>();
      List<ZNRecord> recordList = dataAccessor
          .getClusterPropertyList(ClusterPropertyType.EXTERNALVIEW);
      for (ZNRecord record : recordList)
      {
        ExternalView view = new ExternalView(record);
        Set<String> resourceKeys = view.getResourceKeys();
        for (String resourceKeyName : resourceKeys)
        {
          Map<String, String> stateMap = view.getStateMap(resourceKeyName);
          for (String name : stateMap.keySet())
          {
            Map<String,String> row = new HashMap<String, String>();
            row.put("instanceName", name);
            row.put("resourceGroup", resourceGroup);
            row.put("state", stateMap.get(name));
            row.put("resourceKey", name);
            rows.add(row);
          }
        }
      }
      List<Map<String,String>> messagesToSend = _evaluator.evaluateCriteria(rows,recipientCriteria);
      for (Map<String, String> map : messagesToSend)
      {
        
        
      }
    }

    return 0;
  }

  @Override
  public boolean send(Criteria receipientCriteria, Message message,
      AsyncCallback callbackOnReply)
  {
    return false;
  }

  
}
