package org.apache.helix.util;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestInstanceValidationUtil {
  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_INSTANCE = "instance0";
  private static final PropertyKey.Builder BUILDER = new PropertyKey.Builder(TEST_CLUSTER);

  @DataProvider
  Object[][] isEnabledTestSuite() {
    return new Object[][] {
        {
            true, true, true
        }, {
            true, false, false
        }, {
            false, true, false
        }, {
            false, false, false
        }
    };
  }

  @Test(dataProvider = "isEnabledTestSuite")
  public void TestIsInstanceEnabled(boolean instanceConfigEnabled, boolean clusterConfigEnabled,
      boolean expected) {
    Mock mock = new Mock();
    InstanceConfig instanceConfig = new InstanceConfig(TEST_INSTANCE);
    instanceConfig.setInstanceEnabled(instanceConfigEnabled);
    doReturn(instanceConfig).when(mock.dataAccessor)
        .getProperty(BUILDER.instanceConfig(TEST_INSTANCE));
    ClusterConfig clusterConfig = new ClusterConfig(TEST_CLUSTER);
    if (!clusterConfigEnabled) {
      clusterConfig.setDisabledInstances(ImmutableMap.of(TEST_INSTANCE, "12345"));
    }
    doReturn(clusterConfig).when(mock.dataAccessor)
        .getProperty(BUILDER.clusterConfig());

    boolean isEnabled = InstanceValidationUtil.isEnabled(mock.dataAccessor, TEST_INSTANCE);

    Assert.assertEquals(isEnabled, expected);
  }

  @Test(expectedExceptions = HelixException.class)
  public void TestIsInstanceEnabled_whenInstanceConfigNull() {
    Mock mock = new Mock();
    doReturn(null).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.CONFIGS)));

    InstanceValidationUtil.isEnabled(mock.dataAccessor, TEST_INSTANCE);
  }

  @Test(expectedExceptions = HelixException.class)
  public void TestIsInstanceEnabled_whenClusterConfigNull() {
    Mock mock = new Mock();
    doReturn(new InstanceConfig(TEST_INSTANCE)).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.CONFIGS)));
    doReturn(null).when(mock.dataAccessor)
        .getProperty(BUILDER.clusterConfig());

    InstanceValidationUtil.isEnabled(mock.dataAccessor, TEST_INSTANCE);
  }

  @Test
  public void TestIsInstanceAlive() {
    Mock mock = new Mock();
    doReturn(new LiveInstance(TEST_INSTANCE)).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.LIVEINSTANCES)));

    Assert.assertTrue(InstanceValidationUtil.isAlive(mock.dataAccessor, TEST_INSTANCE));
  }

  @Test
  public void TestHasResourceAssigned_success() {
    String sessionId = "sessionId";
    String resource = "db";
    Mock mock = new Mock();
    LiveInstance liveInstance = new LiveInstance(TEST_INSTANCE);
    liveInstance.setSessionId(sessionId);
    doReturn(liveInstance).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.LIVEINSTANCES)));
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.CURRENTSTATES)));
    CurrentState currentState = mock(CurrentState.class);
    when(currentState.getPartitionStateMap()).thenReturn(ImmutableMap.of("db0", "master"));
    doReturn(currentState).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.CURRENTSTATES)));

    Assert.assertTrue(
        InstanceValidationUtil.hasResourceAssigned(mock.dataAccessor, TEST_CLUSTER, TEST_INSTANCE));
  }

  @Test
  public void TestHasResourceAssigned_fail() {
    String sessionId = "sessionId";
    String resource = "db";
    Mock mock = new Mock();
    LiveInstance liveInstance = new LiveInstance(TEST_INSTANCE);
    liveInstance.setSessionId(sessionId);
    doReturn(liveInstance).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.LIVEINSTANCES)));
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.CURRENTSTATES)));
    CurrentState currentState = mock(CurrentState.class);
    when(currentState.getPartitionStateMap()).thenReturn(Collections.<String, String> emptyMap());
    doReturn(currentState).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.CURRENTSTATES)));

    Assert.assertFalse(
        InstanceValidationUtil.hasResourceAssigned(mock.dataAccessor, TEST_CLUSTER, TEST_INSTANCE));
  }

  @Test
  public void TestHasResourceAssigned_whenNotAlive() {
    Mock mock = new Mock();
    doReturn(null).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.LIVEINSTANCES)));

    Assert.assertFalse(
        InstanceValidationUtil.hasResourceAssigned(mock.dataAccessor, TEST_CLUSTER, TEST_INSTANCE));
  }

  @Test
  public void TestHasDisabledPartitions_false() {
    Mock mock = new Mock();
    InstanceConfig instanceConfig = mock(InstanceConfig.class);
    when(instanceConfig.getDisabledPartitionsMap())
        .thenReturn(Collections.<String, List<String>> emptyMap());
    when(mock.dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(instanceConfig);

    Assert.assertFalse(InstanceValidationUtil.hasDisabledPartitions(mock.dataAccessor, TEST_CLUSTER,
        TEST_INSTANCE));
  }

  @Test
  public void TestHasDisabledPartitions_with_only_names() {
    Mock mock = new Mock();
    InstanceConfig instanceConfig = mock(InstanceConfig.class);
    when(instanceConfig.getDisabledPartitionsMap())
        .thenReturn(ImmutableMap.of("db0", Collections.emptyList()));
    when(mock.dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(instanceConfig);

    Assert.assertFalse(InstanceValidationUtil.hasDisabledPartitions(mock.dataAccessor, TEST_CLUSTER,
        TEST_INSTANCE));
  }

  @Test
  public void TestHasDisabledPartitions_true() {
    Mock mock = new Mock();
    InstanceConfig instanceConfig = mock(InstanceConfig.class);
    when(instanceConfig.getDisabledPartitionsMap())
        .thenReturn(ImmutableMap.of("db0", Arrays.asList("p1")));
    when(mock.dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(instanceConfig);

    Assert.assertTrue(InstanceValidationUtil.hasDisabledPartitions(mock.dataAccessor, TEST_CLUSTER,
        TEST_INSTANCE));
  }

  @Test(expectedExceptions = HelixException.class)
  public void TestHasDisabledPartitions_exception() {
    Mock mock = new Mock();
    when(mock.dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(null);

    Assert.assertTrue(InstanceValidationUtil.hasDisabledPartitions(mock.dataAccessor, TEST_CLUSTER,
        TEST_INSTANCE));
  }

  @Test
  public void TestHasValidConfig_true() {
    Mock mock = new Mock();
    when(mock.dataAccessor.getProperty(any(PropertyKey.class)))
        .thenReturn(new InstanceConfig(TEST_INSTANCE));

    Assert.assertTrue(
        InstanceValidationUtil.hasValidConfig(mock.dataAccessor, TEST_CLUSTER, TEST_INSTANCE));
  }

  @Test
  public void TestHasValidConfig_false() {
    Mock mock = new Mock();
    when(mock.dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(null);

    Assert.assertFalse(
        InstanceValidationUtil.hasValidConfig(mock.dataAccessor, TEST_CLUSTER, TEST_INSTANCE));
  }

  @Test
  public void TestHasErrorPartitions_true() {
    String sessionId = "sessionId";
    String resource = "db";
    Mock mock = new Mock();
    LiveInstance liveInstance = new LiveInstance(TEST_INSTANCE);
    liveInstance.setSessionId(sessionId);
    doReturn(liveInstance).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.LIVEINSTANCES)));
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.CURRENTSTATES)));
    CurrentState currentState = mock(CurrentState.class);
    when(currentState.getPartitionStateMap()).thenReturn(ImmutableMap.of("db0", "ERROR"));
    doReturn(currentState).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.CURRENTSTATES)));

    Assert.assertTrue(
        InstanceValidationUtil.hasErrorPartitions(mock.dataAccessor, TEST_CLUSTER, TEST_INSTANCE));
  }

  @Test
  public void TestHasErrorPartitions_false() {
    String sessionId = "sessionId";
    String resource = "db";
    Mock mock = new Mock();
    LiveInstance liveInstance = new LiveInstance(TEST_INSTANCE);
    liveInstance.setSessionId(sessionId);
    doReturn(liveInstance).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.LIVEINSTANCES)));
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.CURRENTSTATES)));
    CurrentState currentState = mock(CurrentState.class);
    when(currentState.getPartitionStateMap()).thenReturn(ImmutableMap.of("db0", "Master"));
    doReturn(currentState).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.CURRENTSTATES)));

    Assert.assertFalse(
        InstanceValidationUtil.hasErrorPartitions(mock.dataAccessor, TEST_CLUSTER, TEST_INSTANCE));
  }

  @Test(expectedExceptions = HelixException.class)
  public void TestIsInstanceStable_exception_whenPersistAssignmentOff() {
    Mock mock = new Mock();
    ClusterConfig clusterConfig = new ClusterConfig(TEST_CLUSTER);
    clusterConfig.setPersistIntermediateAssignment(false);
    when(mock.dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(clusterConfig);

    InstanceValidationUtil.isInstanceStable(mock.dataAccessor, TEST_INSTANCE);
  }

  @Test(expectedExceptions = HelixException.class)
  public void TestIsInstanceStable_exception_whenExternalViewNull() {
    String resource = "db";
    Mock mock = new Mock();
    ClusterConfig clusterConfig = new ClusterConfig(TEST_CLUSTER);
    clusterConfig.setPersistIntermediateAssignment(true);
    doReturn(clusterConfig).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.CONFIGS)));
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(true);
    when(idealState.getPartitionSet()).thenReturn(ImmutableSet.of("db0"));
    when(idealState.getInstanceStateMap("db0"))
        .thenReturn(ImmutableMap.of(TEST_INSTANCE, "Master"));
    when(idealState.isValid()).thenReturn(true);
    when(idealState.getStateModelDefRef()).thenReturn("MasterSlave");
    doReturn(idealState).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    doReturn(null).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    InstanceValidationUtil.isInstanceStable(mock.dataAccessor, TEST_INSTANCE);
  }

  @Test
  public void TestIsInstanceStable_true() {
    String resource = "db";
    Mock mock = new Mock();
    ClusterConfig clusterConfig = new ClusterConfig(TEST_CLUSTER);
    clusterConfig.setPersistIntermediateAssignment(true);
    doReturn(clusterConfig).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.CONFIGS)));
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(Boolean.TRUE);
    when(idealState.getPartitionSet()).thenReturn(ImmutableSet.of("db0"));
    when(idealState.getInstanceStateMap("db0"))
        .thenReturn(ImmutableMap.of(TEST_INSTANCE, "Master"));
    idealState.setInstanceStateMap("db0", ImmutableMap.of(TEST_INSTANCE, "Master"));
    doReturn(idealState).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    ExternalView externalView = new ExternalView(resource);
    externalView.setStateMap("db0", ImmutableMap.of(TEST_INSTANCE, "Master"));
    doReturn(externalView).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));

    boolean result = InstanceValidationUtil.isInstanceStable(mock.dataAccessor, TEST_INSTANCE);
    Assert.assertTrue(result);
  }

  @Test(description = "IdealState: slave state, ExternalView:Master state")
  public void TestIsInstanceStable_false() {
    String resource = "db";
    Mock mock = new Mock();
    ClusterConfig clusterConfig = new ClusterConfig(TEST_CLUSTER);
    clusterConfig.setPersistIntermediateAssignment(true);
    doReturn(clusterConfig).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.CONFIGS)));
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(true);
    when(idealState.getPartitionSet()).thenReturn(ImmutableSet.of("db0"));
    when(idealState.getInstanceStateMap("db0")).thenReturn(ImmutableMap.of(TEST_INSTANCE, "slave"));
    when(idealState.isValid()).thenReturn(true);
    when(idealState.getStateModelDefRef()).thenReturn("MasterSlave");
    doReturn(idealState).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    ExternalView externalView = new ExternalView(resource);
    externalView.setStateMap("db0", ImmutableMap.of(TEST_INSTANCE, "Master"));
    doReturn(externalView).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));

    boolean result = InstanceValidationUtil.isInstanceStable(mock.dataAccessor, TEST_INSTANCE);
    Assert.assertFalse(result);
  }

  @Test
  public void TestSiblingNodesActiveReplicaCheck_success() {
    String resource = "resource";
    Mock mock = new Mock();
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    // set ideal state
    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(true);
    when(idealState.isValid()).thenReturn(true);
    when(idealState.getStateModelDefRef()).thenReturn("MasterSlave");
    doReturn(idealState).when(mock.dataAccessor).getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));

    // set external view
    ExternalView externalView = mock(ExternalView.class);
    when(externalView.getMinActiveReplicas()).thenReturn(2);
    when(externalView.getStateModelDefRef()).thenReturn("MasterSlave");
    when(externalView.getPartitionSet()).thenReturn(ImmutableSet.of("db0"));
    when(externalView.getStateMap("db0")).thenReturn(ImmutableMap.of(TEST_INSTANCE, "Master",
        "instance1", "Slave", "instance2", "Slave", "instance3", "Slave"));
    doReturn(externalView).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    StateModelDefinition stateModelDefinition = mock(StateModelDefinition.class);
    when(stateModelDefinition.getInitialState()).thenReturn("OFFLINE");
    doReturn(stateModelDefinition).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.STATEMODELDEFS)));

    boolean result =
        InstanceValidationUtil.siblingNodesActiveReplicaCheck(mock.dataAccessor, TEST_INSTANCE);

    Assert.assertTrue(result);
  }

  @Test
  public void TestSiblingNodesActiveReplicaCheck_fail() {
    String resource = "resource";
    Mock mock = new Mock();
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    // set ideal state
    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(true);
    when(idealState.isValid()).thenReturn(true);
    when(idealState.getStateModelDefRef()).thenReturn("MasterSlave");
    doReturn(idealState).when(mock.dataAccessor).getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));

    ExternalView externalView = mock(ExternalView.class);
    when(externalView.getMinActiveReplicas()).thenReturn(3);
    when(externalView.getStateModelDefRef()).thenReturn("MasterSlave");
    when(externalView.getPartitionSet()).thenReturn(ImmutableSet.of("db0"));
    when(externalView.getStateMap("db0")).thenReturn(
        ImmutableMap.of(TEST_INSTANCE, "Master", "instance1", "Slave", "instance2", "Slave"));
    doReturn(externalView).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    StateModelDefinition stateModelDefinition = mock(StateModelDefinition.class);
    when(stateModelDefinition.getInitialState()).thenReturn("OFFLINE");
    doReturn(stateModelDefinition).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.STATEMODELDEFS)));

    boolean result =
        InstanceValidationUtil.siblingNodesActiveReplicaCheck(mock.dataAccessor, TEST_INSTANCE);

    Assert.assertFalse(result);
  }

  @Test
  public void TestSiblingNodesActiveReplicaCheck_whenNoMinActiveReplica() {
    String resource = "resource";
    Mock mock = new Mock();
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    // set ideal state
    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(true);
    when(idealState.isValid()).thenReturn(true);
    when(idealState.getStateModelDefRef()).thenReturn("MasterSlave");
    doReturn(idealState).when(mock.dataAccessor).getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    //set externalView
    ExternalView externalView = mock(ExternalView.class);
    // the resource sibling check will be skipped by design
    when(externalView.getMinActiveReplicas()).thenReturn(-1);
    doReturn(externalView).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));

    boolean result = InstanceValidationUtil.siblingNodesActiveReplicaCheck(mock.dataAccessor, TEST_INSTANCE);
    Assert.assertTrue(result);
  }

  @Test(expectedExceptions = HelixException.class)
  public void TestSiblingNodesActiveReplicaCheck_exception_whenExternalViewUnavailable() {
    String resource = "resource";
    Mock mock = new Mock();
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor)
        .getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    // set ideal state
    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(true);
    when(idealState.isValid()).thenReturn(true);
    when(idealState.getStateModelDefRef()).thenReturn("MasterSlave");
    doReturn(idealState).when(mock.dataAccessor).getProperty(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));

    doReturn(null).when(mock.dataAccessor)
        .getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));

    InstanceValidationUtil.siblingNodesActiveReplicaCheck(mock.dataAccessor, TEST_INSTANCE);
  }

  private class Mock {
    HelixDataAccessor dataAccessor;
    ConfigAccessor configAccessor;

    Mock() {
      this.dataAccessor = mock(HelixDataAccessor.class);
      this.configAccessor = mock(ConfigAccessor.class);
      when(dataAccessor.keyBuilder()).thenReturn(BUILDER);
    }
  }

  public static class PropertyKeyArgument extends ArgumentMatcher<PropertyKey> {
    private PropertyType propertyType;

    public PropertyKeyArgument(PropertyType propertyType) {
      this.propertyType = propertyType;
    }

    @Override
    public boolean matches(Object o) {
      PropertyKey propertyKey = (PropertyKey) o;

      return this.propertyType == propertyKey.getType();
    }
  }
}
