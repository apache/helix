package com.linkedin.helix.integration;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.healthcheck.ParticipantHealthReportCollectorImpl;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.mock.storage.MockEspressoHealthReportProvider;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockTransition;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestAddDropAlert extends ZkIntegrationTestBase
{
  ZkClient _zkClient;
  protected ClusterSetup _setupTool = null;
  protected final String _alertStr = "EXP(accumulate()(localhost_12918.RestQueryStats@DBName=TestDB0.latency))CMP(GREATER)CON(10)";
  protected final String _alertStatusStr = _alertStr; //+" : (*)";
  protected final String _dbName = "TestDB0";

  @BeforeClass ()
  public void beforeClass() throws Exception
  {
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());

    _setupTool = new ClusterSetup(ZK_ADDR);
  }

  @AfterClass
  public void afterClass()
  {
    _zkClient.close();
  }

  public class AddDropAlertTransition extends MockTransition
  {
    @Override
    public void doTransition(Message message, NotificationContext context)
    {
      HelixManager manager = context.getManager();
      DataAccessor accessor = manager.getDataAccessor();
      String fromState = message.getFromState();
      String toState = message.getToState();
      String instance = message.getTgtName();
      String partition = message.getPartitionName();

      if (fromState.equalsIgnoreCase("SLAVE")
          && toState.equalsIgnoreCase("MASTER"))
      {

    	//add a stat and report to ZK
    	//perhaps should keep reporter per instance...
    	ParticipantHealthReportCollectorImpl reporter =
    			new ParticipantHealthReportCollectorImpl(manager, instance);
    	MockEspressoHealthReportProvider provider = new
    			MockEspressoHealthReportProvider();
    	reporter.addHealthReportProvider(provider);
    	String statName = "latency";
    	provider.setStat(_dbName, statName,"15");
    	reporter.transmitHealthReports();

    	//sleep long enough for first set of alerts to report and alert to get deleted
    	//then change reported data
    	try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			System.err.println("Error sleeping");
		}
    	provider.setStat(_dbName, statName,"1");
    	reporter.transmitHealthReports();


    	/*
        for (int i = 0; i < 5; i++)
        {
          accessor.setProperty(PropertyType.HEALTHREPORT,
                               new ZNRecord("mockAlerts" + i),
                               instance,
                               "mockAlerts");
          try
          {
            Thread.sleep(1000);
          }
          catch (InterruptedException e)
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        */
      }
    }
  }

  @Test()
  public void testAddDropAlert() throws Exception
  {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START TestAddDropAlert at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName,
                            ZK_ADDR,
                            12918,        // participant start port
                            "localhost",  // participant name prefix
                            "TestDB",     // resource name prefix
                            1,            // resources
                            10,           // partitions per resource group
                            5,            // number of nodes //change back to 5!!!
                            1,            // replicas //change back to 3!!!
                            "MasterSlave",
                            true);        // do rebalance

    _setupTool.getClusterManagementTool().addAlert(clusterName, _alertStr);

    TestHelper.startController(clusterName,
                               "controller_0",
                               ZK_ADDR,
                               HelixControllerMain.STANDALONE);
    // start participants
    for (int i = 0; i < 5; i++) //!!!change back to 5
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName,
                                            instanceName,
                                            ZK_ADDR,
                                            new AddDropAlertTransition());
      new Thread(participants[i]).start();
    }


    //drop alert soon after adding, but leave enough time for alert to fire once
    Thread.sleep(5000);
    _setupTool.getClusterManagementTool().dropAlert(clusterName, _alertStr);


    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // other verifications go here
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    //for (int i = 0; i < 1; i++) //change 1 back to 5
    //{
      //String instance = "localhost_" + (12918 + i);
      String instance = "localhost_12918";
      ZNRecord record = accessor.getProperty(PropertyType.ALERT_STATUS);
      Map<String, Map<String,String>> recMap = record.getMapFields();
      Set<String> keySet = recMap.keySet();
      Assert.assertEquals(keySet.size(), 0);
    //}

    System.out.println("END TestAddDropAlert at " + new Date(System.currentTimeMillis()));
  }
}
