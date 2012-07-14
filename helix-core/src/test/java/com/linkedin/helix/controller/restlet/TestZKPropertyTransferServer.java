package com.linkedin.helix.controller.restlet;

import java.io.StringWriter;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.Client;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.controller.restlet.ZNRecordUpdate.OpCode;
import com.linkedin.helix.integration.ZkStandAloneCMTestBase;

public class TestZKPropertyTransferServer extends ZkStandAloneCMTestBase
{
  private static Logger LOG =
      Logger.getLogger(TestZKPropertyTransferServer.class);

  @Test
  public void TestHelixPropertyforwarding()
  {
    String participant1 = "localhost_" + START_PORT;
    String participant2 = "localhost_" + (START_PORT + 1);
    
    ZKPropertyTransferServer.PERIOD = 1000;
    
    ZNRecord healthRecord1 = new ZNRecord("TestStat");
    healthRecord1.setSimpleField("TestKey", "TestValue");
    
    ZNRecord healthRecord2 = new ZNRecord("TestStat");
    healthRecord2.setSimpleField("TestKey", "TestValue2");
    healthRecord2.setSimpleField("TestKey1", "TestValue3");
    
    ZNRecord healthRecord3 = new ZNRecord("TestStat");
    healthRecord3.setSimpleField("TestKey", "TestValue3");
    healthRecord3.setSimpleField("TestKey3", "TestValue4");
    
    Builder kb = _startCMResultMap.get(participant1)._manager.getHelixDataAccessor().keyBuilder();
    
    String path = kb.healthReport(participant1, "TestKey").getPath();
    
    String path2 = kb.stateTransitionStatus(participant1, "123", "DB43").getPath();
    ZNRecord suRecord2 = new ZNRecord("TestStatusUpdate");
    suRecord2.setSimpleField("TestKey1", "TestValue1");
    ZNRecord suRecord3 = new ZNRecord("TestStatusUpdate");
    suRecord3.setSimpleField("TestKey2", "TestValue2");
    ZNRecord suRecord4 = new ZNRecord("TestStatusUpdate");
    suRecord4.setSimpleField("TestKey3", "TestValue3");
    
    String path3 = kb.stateTransitionStatus(participant1, "123", "DB43_2", "TestStatusUpdate2").getPath();
    ZNRecord suRecord5 = new ZNRecord("TestStatusUpdate2");
    suRecord5.setSimpleField("TestKey4", "TestValue4");
    
    sendZNRecordData(healthRecord1, path, OpCode.SET );
    sendZNRecordData(suRecord2, path2, OpCode.UPDATE );
    sendZNRecordData(suRecord3, path2, OpCode.UPDATE );
    sendZNRecordData(suRecord4, path2, OpCode.UPDATE );
    sendZNRecordData(suRecord5, path3, OpCode.UPDATE );
    sendZNRecordData(healthRecord2, path, OpCode.SET);
    sendZNRecordData(healthRecord3, path, OpCode.SET);
    
    try
    {
      Thread.sleep(20000);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    HelixDataAccessor accessor = _startCMResultMap.get(participant2)._manager.getHelixDataAccessor();
    ZNRecord result1 = accessor.getProperty(accessor.keyBuilder().healthReport(participant1, "TestKey")).getRecord();
    Assert.assertTrue(result1.getSimpleField("TestKey").equals(healthRecord3.getSimpleField("TestKey")));
    Assert.assertTrue(result1.getSimpleField("TestKey2")== null);
    Assert.assertTrue(result1.getSimpleField("TestKey3").equals(healthRecord3.getSimpleField("TestKey3")));
    
    ZNRecord su1 = accessor.getProperty(accessor.keyBuilder().stateTransitionStatus(participant1, "123", "DB43")).getRecord();
    Assert.assertTrue(su1.getSimpleField("TestKey1").equals("TestValue1"));
    Assert.assertTrue(su1.getSimpleField("TestKey2").equals("TestValue2"));
    Assert.assertTrue(su1.getSimpleField("TestKey3").equals("TestValue3"));
    
    ZNRecord su2 = accessor.getProperty(accessor.keyBuilder().stateTransitionStatus(participant1, "123", "DB43_2", "TestStatusUpdate2")).getRecord();
    Assert.assertTrue(su2.getSimpleField("TestKey4").equals("TestValue4"));
    
    ZKPropertyTransferServer.getInstance().shutdown();
  }
  

  // TODO: reslet 1.1.10 does not provide async request. We can write a netty client for this.
  public static void sendZNRecordData(ZNRecord record, String path, OpCode code)
  {
    ZNRecordUpdate update = new ZNRecordUpdate(path, code, record);
    Reference resourceRef = new Reference(ZKPropertyTransferServer.getInstance().getWebserviceUrl());
    Request request = new Request(Method.PUT, resourceRef);
    
    ObjectMapper mapper = new ObjectMapper();
    StringWriter sw = new StringWriter();
    try
    {
      mapper.writeValue(sw, update);
    }
    catch (Exception e)
    {
      LOG.error("",e);
    }

    request.setEntity(
        ZNRecordUpdateResource.UPDATEKEY + "=" + sw, MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);
    
    if(response.getStatus() != Status.SUCCESS_OK)
    {
      LOG.error("Status : " + response.getStatus());
    }
  }
}
