package com.linkedin.helix.mock.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.healthcheck.StatHealthReportProvider;
import com.linkedin.helix.manager.zk.ZKHelixManager;
import com.linkedin.helix.mock.consumer.ConsumerAdapter;
import com.linkedin.helix.mock.consumer.RelayConfig;
import com.linkedin.helix.mock.consumer.RelayConsumer;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;

public class HealthCheckStateModel extends StateModel
{

  // private Map<Integer, RelayConsumer> relayConsumersMap;
  private RelayConsumer consumer = null;
  private RelayConfig relayConfig;
  private StorageAdapter storage;
  private StatHealthReportProvider _provider;
  //private StatReporterThread _reporterThread;
  private int _reportInterval;
  private Map<String, Vector<String>> _reportValues;
  private CountDownLatch _countdown;

  private static Logger logger = Logger.getLogger(HealthCheckStateModel.class);

  public HealthCheckStateModel(String stateUnitKey, StorageAdapter storageAdapter, StatHealthReportProvider provider,
		  int reportInterval, Map<String, Vector<String>> reportValues, CountDownLatch countdown)
  {
    // relayConsumersMap = new HashMap<Integer,RelayConsumer>();
    storage = storageAdapter;
   //_reporterThread = new StatReporterThread(provider, reportInterval, reportValues, countdown);
    // this.consumerAdapter = consumerAdapter;
   _provider = provider;
   _reportInterval = reportInterval;
   _reportValues = reportValues;
   _countdown = countdown;
  }

  public RelayConfig getRelayConfig()
  {
    return relayConfig;
  }

  public void setRelayConfig(RelayConfig relayConfig)
  {
    this.relayConfig = relayConfig;
  }

  void checkDebug(Message task) throws Exception
  {
    // For debugging purposes
    if ((Boolean) task.getDebug() == true)
    {
      throw new Exception("Exception for debug");
    }
  }

  // @transition(to='to',from='from',blah blah..)
  public void onBecomeSlaveFromOffline(Message task, NotificationContext context)
      throws Exception
  {

    logger.info("Becoming slave from offline");

    checkDebug(task);

    String partition = (String) task.getStateUnitKey();
    String[] pdata = partition.split("\\.");
    String dbName = pdata[0];

    // Initializations for the storage node to create right tables, indexes
    // etc.
    storage.init(partition);
    storage.setPermissions(partition, "READONLY");

    // start consuming from the relay
    consumer = storage.getNewRelayConsumer(dbName, partition);
    consumer.start();
    // TODO: how do we know we are caught up?

    logger.info("Became slave for partition " + partition);
  }

  // @transition(to='to',from='from',blah blah..)
  public void onBecomeSlaveFromMaster(Message task, NotificationContext context)
      throws Exception
  {

    logger.info("Becoming slave from master");

    checkDebug(task);

    String partition = (String) task.getStateUnitKey();
    String[] pdata = partition.split("\\.");
    String dbName = pdata[0];
    storage.setPermissions(partition, "READONLY");
    storage.waitForWrites(partition);

    // start consuming from the relay
    consumer = storage.getNewRelayConsumer(dbName, partition);
    consumer.start();

    logger.info("Becamse slave for partition " + partition);
  }

  // @transition(to='to',from='from',blah blah..)
  public void onBecomeMasterFromSlave(Message task, NotificationContext context)
      throws Exception
  {
    logger.info("Becoming master from slave");

    checkDebug(task);

    String partition = (String) task.getStateUnitKey();

    // stop consumer and refetch from all so all changes are drained
    consumer.flush(); // blocking call

    // TODO: publish the hwm somewhere
    long hwm = consumer.getHwm();
    storage.setHwm(partition, hwm);
    storage.removeConsumer(partition);
    consumer = null;

    // set generation in storage
    Integer generationId = (Integer) task.getGeneration();
    storage.setGeneration(partition, generationId);

    storage.setPermissions(partition, "READWRITE");

    String[] pdata = partition.split("\\.");
    String dbName = pdata[0];
    
    HelixManager manager = context.getManager();
    
    //start the reporting thread
    logger.debug("Starting stats reporting thread");
    StatReporterThread reporterThread = new StatReporterThread(manager, _provider, dbName, partition, 
    										_reportInterval, _reportValues, _countdown);
    Thread t = new Thread(reporterThread);
    t.run();
    logger.info("Became master for partition " + partition);
  }

  // @transition(to='to',from='from',blah blah..)
  public void onBecomeOfflineFromSlave(Message task, NotificationContext context)
      throws Exception
  {

    logger.info("Becoming offline from slave");

    checkDebug(task);

    String partition = (String) task.getStateUnitKey();

    consumer.stop();
    storage.removeConsumer(partition);
    consumer = null;

    storage.setPermissions(partition, "OFFLINE");

    logger.info("Became offline for partition " + partition);
  }
  
  public static String formStatName(String dbName, String partitionName, String metricName)
	{
		String statName;
		statName = "db"+dbName+".partition"+partitionName+"."+metricName;
		return statName;	
	}
  
  public class StatReporterThread implements Runnable
  {
	  private HelixManager _manager;
	  private int _reportInterval;
	  private Map<String, Vector<String>> _reportValues;
	  private CountDownLatch _countdown;
	  private StatHealthReportProvider _provider;
	  private String _dbName;
	  private String _partitionName;
	  
	public StatReporterThread(HelixManager manager, StatHealthReportProvider provider, String dbName, 
			String partitionName, int reportInterval, 
			Map<String,Vector<String>> reportValues, CountDownLatch countdown)
	{
		_manager = manager;
		_reportInterval = reportInterval;
		_reportValues = reportValues;
		_countdown = countdown;
		_provider = provider;
		_dbName = dbName;
		_partitionName = partitionName;
	}
	  
	@Override
	public void run() 
	{
		boolean doneWithStats = false;
		while (!doneWithStats) {
			doneWithStats = true;
			try {
				Thread.sleep(_reportInterval);
			} catch (InterruptedException e) {
				logger.error("Unable to sleep, stats not getting staggered, "+e);
			}
			for (String metricName : _reportValues.keySet()) {
				Vector<String> currValues = _reportValues.get(metricName);
				if (currValues.size() > 0) {
					doneWithStats = false;
					String statName = formStatName(_dbName, _partitionName, metricName);
					String currValue = currValues.remove(0);
					Long currTimestamp = System.currentTimeMillis();
					_provider.writeStat(statName, currValue, String.valueOf(currTimestamp));
				}
			}
			_manager.getHealthReportCollector().transmitHealthReports();
		}

		_countdown.countDown();
	}
	  
  }
}
