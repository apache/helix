package com.linkedin.clustermanager.messaging;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.model.Message;

public class TestAsyncCallback
{
  class AsyncCallbackSample extends AsyncCallback
  {
    int _onTimeOutCalled = 0;
    int _onReplyMessageCalled = 0;
    @Override
    public void onTimeOut()
    {
      // TODO Auto-generated method stub
      _onTimeOutCalled ++;
    }

    @Override
    public void onReplyMessage(Message message)
    {
      _onReplyMessageCalled++;
    }
  }
  
  @Test(groups =
  { "unitTest" })
  public void testAsyncCallback() throws Exception
  {
    System.out.println("START TestAsyncCallback at" + new Date(System.currentTimeMillis()));
    AsyncCallbackSample callback = new AsyncCallbackSample();
    AssertJUnit.assertFalse(callback.isInterrupted());
    AssertJUnit.assertFalse(callback.isTimedOut());
    AssertJUnit.assertTrue(callback.getMessageReplied().size() == 0);

    int nMsgs = 5;
    
    List<Message> messageSent = new ArrayList<Message>();
    for(int i = 0;i < nMsgs; i++)
    {
      messageSent.add(new Message("Test", UUID.randomUUID().toString()));
    }

    callback.setMessagesSent(messageSent);
    
    for(int i = 0;i < nMsgs; i++)
    {
      AssertJUnit.assertFalse(callback.isDone());
      callback.onReply(new Message("TestReply", UUID.randomUUID().toString()));
    }
    AssertJUnit.assertTrue(callback.isDone());

    AssertJUnit.assertTrue(callback._onTimeOutCalled == 0 );
    
    sleep(50);
    callback = new AsyncCallbackSample();
    callback.setMessagesSent(messageSent);
    callback.setTimeout(1000);
    sleep(50);
    callback.startTimer();
    AssertJUnit.assertFalse(callback.isTimedOut());
    for(int i = 0;i < nMsgs - 1; i++)
    {
      sleep(50);
      AssertJUnit.assertFalse(callback.isDone());
      AssertJUnit.assertTrue(callback._onReplyMessageCalled == i);
      callback.onReply(new Message("TestReply", UUID.randomUUID().toString()));
    }
    sleep(1000);
    AssertJUnit.assertTrue(callback.isTimedOut());
    AssertJUnit.assertTrue(callback._onTimeOutCalled == 1 );
    AssertJUnit.assertFalse(callback.isDone());
    
    callback = new AsyncCallbackSample();
    callback.setMessagesSent(messageSent);
    callback.setTimeout(1000);
    callback.startTimer();
    sleep(50);
    AssertJUnit.assertFalse(callback.isTimedOut());
    for(int i = 0;i < nMsgs; i++)
    {
      AssertJUnit.assertFalse(callback.isDone());
      sleep(50);
      AssertJUnit.assertTrue(callback._onReplyMessageCalled == i);
      callback.onReply(new Message("TestReply", UUID.randomUUID().toString()));
    }
    AssertJUnit.assertTrue(callback.isDone());
    sleep(1300);
    AssertJUnit.assertFalse(callback.isTimedOut());
    AssertJUnit.assertTrue(callback._onTimeOutCalled == 0 );
    System.out.println("END TestAsyncCallback at" + new Date(System.currentTimeMillis()));
  }
  
  void sleep(int time)
  {
    try
    {
      Thread.sleep(time);
    }
    catch(Exception e)
    {
      System.out.println(e);
    }
  }
}
