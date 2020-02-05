package org.apache.helix.integration.spectator;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCustomizedViewProvider extends ZkTestBase {

  static final String STATE_MODEL = BuiltInStateModelDefinitions.MasterSlave.name();
  static final String TEST_DB = "TestDB";
  static final String CLASS_NAME = TestRoutingTableProvider.class.getSimpleName();
  static final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  static final int PARTICIPANT_NUMBER = 3;
  static final int PARTICIPANT_START_PORT = 12918;

  static final int PARTITION_NUMBER = 20;
  static final int REPLICA_NUMBER = 3;

  private static final String RESOURCE_NAME = "TestResource";

  private HelixManager _spectator;
  protected HelixManager _manager;

  private List<MockParticipantManager> _participants = new ArrayList<>();
  private List<String> _instances = new ArrayList<>();
  private ClusterControllerManager _controller;

  private static final AtomicBoolean customizedViewChangeCalled = new AtomicBoolean(false);

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println(
        "START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      String instance = PARTICIPANT_PREFIX + "_" + (PARTICIPANT_START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance);
      _instances.add(instance);
    }

    // start dummy participants
    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _instances.get(i));
      participant.syncStart();
      _participants.add(participant);
    }

    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, TEST_DB, _instances,
        STATE_MODEL, PARTITION_NUMBER, REPLICA_NUMBER);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();



    // start speculator
    _spectator = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "spectator",
        InstanceType.SPECTATOR, ZK_ADDR);
    _spectator.connect();

    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
  }

  @AfterClass
  public void afterClass() {
    // stop participants
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    _spectator.disconnect();
    deleteCluster(CLUSTER_NAME);
    _controller.syncStop();
  }

  @Test(expectedExceptions = HelixException.class)
  public void testExternalViewWithType() {
    Map<PropertyType, List<String>> sourceDataTypes = new HashMap<>();
    sourceDataTypes.put(PropertyType.EXTERNALVIEW, Arrays.asList("typeA"));
    sourceDataTypes.put(PropertyType.CUSTOMIZEDVIEW, Arrays.asList("typeA", "typeB"));
    RoutingTableProvider routingTableProvider;
    routingTableProvider = new RoutingTableProvider(_spectator, sourceDataTypes);
  }

  @Test(dependsOnMethods = "testExternalViewWithType", expectedExceptions = HelixException.class)
  public void testCustomizedViewWithoutType() {
    RoutingTableProvider routingTableProvider;
    routingTableProvider = new RoutingTableProvider(_spectator, PropertyType.CUSTOMIZEDVIEW);
  }

  @Test(dependsOnMethods = "testCustomizedViewWithoutType")
  public void testCustomizedViewCorrectConstructor() throws Exception {
    Map<PropertyType, List<String>> sourceDataTypes = new HashMap<>();
    sourceDataTypes.put(PropertyType.CUSTOMIZEDVIEW, Arrays.asList("typeA"));
    MockRoutingTableProvider routingTableProvider = new MockRoutingTableProvider(_spectator, sourceDataTypes);

    CustomizedView customizedView = new CustomizedView(RESOURCE_NAME);
    customizedView.setState("p1", "h1", "testState");


    // Clear the flag before writing to the Customized View Path
    customizedViewChangeCalled.getAndSet(false);
    String customizedViewPath = PropertyPathBuilder.customizedView(CLUSTER_NAME, "typeA", RESOURCE_NAME);
    _manager.getHelixDataAccessor().getBaseDataAccessor().set(customizedViewPath, customizedView.getRecord(),
        AccessOption.PERSISTENT);

    boolean  onCustomizedViewChangeCalled =
    TestHelper.verify(() -> customizedViewChangeCalled.get(), TestHelper.WAIT_DURATION);
    Assert.assertTrue(onCustomizedViewChangeCalled);

    _manager.getHelixDataAccessor().getBaseDataAccessor().remove(customizedViewPath, AccessOption.PERSISTENT);
  }


  @Test(dependsOnMethods = "testCustomizedViewCorrectConstructor")
  public void testRoutingTableSnapshot() throws Exception {
    Map<PropertyType, List<String>> sourceDataTypes = new HashMap<>();
    sourceDataTypes.put(PropertyType.CUSTOMIZEDVIEW, Arrays.asList("typeA", "typeB"));
    RoutingTableProvider routingTableProvider = new RoutingTableProvider(_spectator, sourceDataTypes);

    CustomizedView customizedView1 = new CustomizedView("Resource1");
    customizedView1.setState("p1", "h1", "testState1");
    customizedView1.setState("p1", "h2", "testState1");
    customizedView1.setState("p2", "h1", "testState2");
    customizedView1.setState("p3", "h2", "testState3");
    String customizedViewPath1 = PropertyPathBuilder.customizedView(CLUSTER_NAME, "typeA", "Resource1");

    CustomizedView customizedView2 = new CustomizedView("Resource2");
    customizedView2.setState("p1", "h3", "testState3");
    customizedView2.setState("p1", "h4", "testState2");
    String customizedViewPath2 = PropertyPathBuilder.customizedView(CLUSTER_NAME, "typeA", "Resource2");


    _manager.getHelixDataAccessor().getBaseDataAccessor().set(customizedViewPath1, customizedView1.getRecord(),
        AccessOption.PERSISTENT);
    _manager.getHelixDataAccessor().getBaseDataAccessor().set(customizedViewPath2, customizedView2.getRecord(),
        AccessOption.PERSISTENT);

    RoutingTableSnapshot routingTableSnapshot = routingTableProvider.getRoutingTableSnapshot(PropertyType.CUSTOMIZEDVIEW, "typeA");
    Assert.assertEquals(routingTableSnapshot.getPropertyType(), PropertyType.CUSTOMIZEDVIEW);
    Assert.assertEquals(routingTableSnapshot.getType(), "typeA");
    routingTableSnapshot = routingTableProvider.getRoutingTableSnapshot(PropertyType.CUSTOMIZEDVIEW, "typeB");
    Assert.assertEquals(routingTableSnapshot.getPropertyType(), PropertyType.CUSTOMIZEDVIEW);
    Assert.assertEquals(routingTableSnapshot.getType(), "typeB");

    routingTableProvider.shutdown();
  }


  class MockRoutingTableProvider extends RoutingTableProvider {
    MockRoutingTableProvider(HelixManager helixManager, Map<PropertyType, List<String>> sourceDataTypes) {
      super(helixManager, sourceDataTypes);
    }

    @Override
    public void onCustomizedViewChange(List<CustomizedView> customizedViewList,
        NotificationContext changeContext){
      customizedViewChangeCalled.getAndSet(true);
      super.onCustomizedViewChange(customizedViewList, changeContext);
    }
  }
}
