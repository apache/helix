package com.linkedin.clustermanager;

import com.linkedin.clustermanager.model.Message;

public abstract class AsyncCallback
{
  public abstract void onReply(Message message);
    
  public abstract boolean isDone();

  public void onTimeOut(){
    
  }
}
