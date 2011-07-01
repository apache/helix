package com.linkedin.clustermanager.mock.controller;

import java.io.IOException;
import java.util.ArrayList;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

public class MockControllerProcess
{

  /**
   * @param args
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonGenerationException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws JsonGenerationException,
      JsonMappingException, InterruptedException, IOException
  {

    MockController storageController = new MockController("cm-instance-0",
        "localhost:2181", "storage-cluster");
    MockController relayController = new MockController("cm-instance-0",
        "localhost:2181", "relay-cluster");

    ArrayList<String> instanceNames = new ArrayList<String>();
    instanceNames.add("relay0");
    instanceNames.add("relay1");
    instanceNames.add("relay2");
    instanceNames.add("relay3");
    instanceNames.add("relay4");

    relayController.createExternalView(instanceNames, 10, 2, "EspressoDB", 0);

    // Messages to initiate offline->slave->master->slave transitions

    storageController.sendMessage("TestMessageId1", "localhost_8900",
        "Offline", "Slave", "EspressoDB.partition-0", 0);
    Thread.sleep(10000);
    storageController.sendMessage("TestMessageId2", "localhost_8900", "Slave",
        "Master", "EspressoDB.partition-0", 0);
    Thread.sleep(10000);
    storageController.sendMessage("TestMessageId3", "localhost_8900", "Master",
        "Slave", "EspressoDB.partition-0", 0);
    Thread.sleep(10000);

    // Change the external view to trigger the consumer to listen from
    // another relay
    relayController.createExternalView(instanceNames, 10, 2, "EspressoDB", 10);

    storageController.sendMessage("TestMessageId4", "localhost_8900", "Slave",
        "Offline", "EspressoDB.partition-0", 0);
    Thread.sleep(10000);
  }

}
