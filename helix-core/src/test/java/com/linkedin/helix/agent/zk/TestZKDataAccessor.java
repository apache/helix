package com.linkedin.helix.agent.zk;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.zookeeper.data.Stat;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.agent.zk.ZKDataAccessor;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.PropertyStoreException;


public class TestZKDataAccessor extends ZkUnitTestBase
{
  private ClusterDataAccessor _accessor;
  private String _clusterName;
  private final String resourceGroup = "resourceGroup";
	private ZkClient _zkClient;

  @Test ()
  public void testSet()
  {
    IdealState idealState = new IdealState(resourceGroup);
    idealState.setNumPartitions(20);
    idealState.setReplicas(2);
    idealState.setStateModelDefRef("StateModel1");
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
    boolean success = _accessor.setProperty(PropertyType.IDEALSTATES, idealState, resourceGroup);
    AssertJUnit.assertTrue(success);
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName, resourceGroup);
    AssertJUnit.assertTrue(_zkClient.exists(path));
    AssertJUnit.assertEquals(idealState.getRecord(), _zkClient.readData(path));

    idealState.setNumPartitions(20);
    success = _accessor.setProperty(PropertyType.IDEALSTATES, idealState, resourceGroup);
    AssertJUnit.assertTrue(success);
    AssertJUnit.assertTrue(_zkClient.exists(path));
    AssertJUnit.assertEquals(idealState.getRecord(), _zkClient.readData(path));
  }

  @Test ()
  public void testGet()
  {
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName, resourceGroup);
    IdealState idealState = new IdealState(resourceGroup);
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());

    _zkClient.delete(path);
    _zkClient.createPersistent(new File(path).getParent(), true);
    _zkClient.createPersistent(path, idealState.getRecord());
    IdealState idealStateRead = _accessor.getProperty(IdealState.class, PropertyType.IDEALSTATES, resourceGroup);
    AssertJUnit.assertEquals(idealState.getRecord(), idealStateRead.getRecord());
  }

  @Test ()
  public void testRemove()
  {
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName, resourceGroup);
    IdealState idealState = new IdealState(resourceGroup);
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());

    _zkClient.delete(path);
    _zkClient.createPersistent(new File(path).getParent(), true);
    _zkClient.createPersistent(path, idealState.getRecord());
    boolean success = _accessor.removeProperty(PropertyType.IDEALSTATES, resourceGroup);
    AssertJUnit.assertTrue(success);
    AssertJUnit.assertFalse(_zkClient.exists(path));
    IdealState idealStateRead = _accessor.getProperty(IdealState.class, PropertyType.IDEALSTATES, resourceGroup);
    AssertJUnit.assertNull(idealStateRead);

  }

  @Test ()
  public void testUpdate()
  {
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName, resourceGroup);
    IdealState idealState = new IdealState(resourceGroup);
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());

    _zkClient.delete(path);
    _zkClient.createPersistent(new File(path).getParent(), true);
    _zkClient.createPersistent(path, idealState.getRecord());
    Stat stat = _zkClient.getStat(path);

    idealState.setIdealStateMode(IdealStateModeProperty.CUSTOMIZED.toString());

    boolean success = _accessor.updateProperty(PropertyType.IDEALSTATES, idealState, resourceGroup);
    AssertJUnit.assertTrue(success);
    AssertJUnit.assertTrue(_zkClient.exists(path));
    ZNRecord value = _zkClient.readData(path);
    AssertJUnit.assertEquals(idealState.getRecord(), value);
    Stat newstat = _zkClient.getStat(path);

    AssertJUnit.assertEquals(stat.getCtime(), newstat.getCtime());
    AssertJUnit.assertNotSame(stat.getMtime(), newstat.getMtime());
    AssertJUnit.assertTrue(stat.getMtime() < newstat.getMtime());
  }

  @Test ()
  public void testGetChildValues()
  {
    List<ExternalView> list = _accessor.getChildValues(ExternalView.class, PropertyType.EXTERNALVIEW, _clusterName);
    AssertJUnit.assertEquals(0, list.size());
  }

  @Test ()
  public void testGetPropertyStore()
  {
    PropertyStore<ZNRecord> store = _accessor.getPropertyStore();
    try
    {
      store.setProperty("child1", new ZNRecord("child1"));
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @BeforeClass
  public void beforeClass() throws IOException, Exception
  {
    _clusterName = CLUSTER_PREFIX + "_" + getShortClassName();

		System.out.println("START TestZKDataAccessor at " + new Date(System.currentTimeMillis()));
		_zkClient = new ZkClient(ZK_ADDR);
		_zkClient.setZkSerializer(new ZNRecordSerializer());

    if (_zkClient.exists("/" + _clusterName))
    {
      _zkClient.deleteRecursive("/" + _clusterName);
    }
    _accessor = new ZKDataAccessor(_clusterName, _zkClient);
  }

  @AfterClass
  public void afterClass()
  {
		_zkClient.close();
		System.out.println("END TestZKDataAccessor at " + new Date(System.currentTimeMillis()));
  }
}
