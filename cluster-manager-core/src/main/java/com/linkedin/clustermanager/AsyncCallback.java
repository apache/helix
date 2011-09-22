package com.linkedin.clustermanager;

import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.model.Message;

public abstract class AsyncCallback
{
  public abstract void onReply(Message message);
    
  public abstract boolean isDone();

  public void onTimeOut(){
    
  }

  public void setMessageSentCount(int messageSentCount)
  {
    
  }

  public void setMessagesSent(Map<InstanceType, List<Message>> generateMessage)
  {
    
  }
  
}
