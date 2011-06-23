package com.linkedin.clustermanager;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


import com.linkedin.clustermanager.Mocks.MockManager;
import com.linkedin.clustermanager.Mocks.MockStateModel;
import com.linkedin.clustermanager.Mocks.MockStateModelAnnotated;
import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.statemachine.CMTaskHandler;

public class TestCMTaskHandler
{
    @Test
    public void testInvocation() throws Exception
    {
        System.out.println("TestCMTaskHandler.testInvocation()");
        Message message = new Message();
        message.setSrcName("cm-instance-0");
        message.setTgtSessionId("1234");
        message.setFromState("Offline");
        message.setToState("Slave");
        message.setStateUnitKey("Teststateunitkey");
        MockStateModel stateModel = new MockStateModel();
        NotificationContext context;

        context = new NotificationContext(new MockManager());
        CMTaskHandler handler;
        handler = new CMTaskHandler(context, message, stateModel);
        handler.call();
        AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    }
    @Test
    public void testInvocationAnnotated() throws Exception
    {
    	System.out.println("TestCMTaskHandler.testInvocation()");
    	Message message = new Message();
    	message.setSrcName("cm-instance-0");
    	message.setTgtSessionId("1234");
    	message.setFromState("Offline");
    	message.setToState("Slave");
    	message.setStateUnitKey("Teststateunitkey");
    	MockStateModelAnnotated stateModel = new MockStateModelAnnotated();
    	NotificationContext context;
    	
    	context = new NotificationContext(new MockManager());
    	CMTaskHandler handler;
    	handler = new CMTaskHandler(context, message, stateModel);
    	handler.call();
    	AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    }

}
