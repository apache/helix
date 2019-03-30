package org.apache.helix.util;

import static org.mockito.Mockito.*;

import java.util.Collections;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.StateModelDefinition;
import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class TestInstanceValidationUtil {
  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_INSTANCE = "instance0";

  @Test
  public void TestSiblingNodesActiveReplicaCheck_success() {
    String resource = "resource";
    Mock mock = new Mock();
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor).getChildNames(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor).getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    ExternalView externalView = mock(ExternalView.class);
    when(externalView.getMinActiveReplicas()).thenReturn(2);
    when(externalView.getStateModelDefRef()).thenReturn("MasterSlave");
    when(externalView.getPartitionSet()).thenReturn(ImmutableSet.of("db0"));
    when(externalView.getStateMap("db0")).thenReturn(ImmutableMap.of(
        TEST_INSTANCE, "Master",
        "instance1", "Slave",
        "instance2", "Slave",
        "instance3", "Slave"));
    doReturn(externalView).when(mock.dataAccessor).getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    StateModelDefinition stateModelDefinition = mock(StateModelDefinition.class);
    when(stateModelDefinition.getInitialState()).thenReturn("OFFLINE");
    doReturn(stateModelDefinition).when(mock.dataAccessor).getProperty(argThat(new PropertyKeyArgument(PropertyType.STATEMODELDEFS)));

    boolean result = InstanceValidationUtil.siblingNodesActiveReplicaCheck(mock.dataAccessor, TEST_INSTANCE);

    Assert.assertTrue(result);
  }

  @Test
  public void TestSiblingNodesActiveReplicaCheck_fail() {
    String resource = "resource";
    Mock mock = new Mock();
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor).getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor).getChildNames(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    ExternalView externalView = mock(ExternalView.class);
    when(externalView.getMinActiveReplicas()).thenReturn(3);
    when(externalView.getStateModelDefRef()).thenReturn("MasterSlave");
    when(externalView.getPartitionSet()).thenReturn(ImmutableSet.of("db0"));
    when(externalView.getStateMap("db0")).thenReturn(ImmutableMap.of(
        TEST_INSTANCE, "Master",
        "instance1", "Slave",
        "instance2", "Slave"));
    doReturn(externalView).when(mock.dataAccessor).getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    StateModelDefinition stateModelDefinition = mock(StateModelDefinition.class);
    when(stateModelDefinition.getInitialState()).thenReturn("OFFLINE");
    doReturn(stateModelDefinition).when(mock.dataAccessor).getProperty(argThat(new PropertyKeyArgument(PropertyType.STATEMODELDEFS)));

    boolean result = InstanceValidationUtil.siblingNodesActiveReplicaCheck(mock.dataAccessor, TEST_INSTANCE);

    Assert.assertFalse(result);
  }

  @Test (expectedExceptions = HelixException.class)
  public void TestSiblingNodesActiveReplicaCheck_exception_whenIdealStatesMisMatch() {
    String resource = "resource";
    Mock mock = new Mock();
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor).getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    doReturn(Collections.emptyList()).when(mock.dataAccessor).getChildNames(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));
    ExternalView externalView = mock(ExternalView.class);
    when(externalView.getMinActiveReplicas()).thenReturn(-1);
    doReturn(externalView).when(mock.dataAccessor).getProperty(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));

    InstanceValidationUtil.siblingNodesActiveReplicaCheck(mock.dataAccessor, TEST_INSTANCE);
  }

  @Test (expectedExceptions = HelixException.class)
  public void TestSiblingNodesActiveReplicaCheck_exception_whenMissingMinActiveReplicas() {
    String resource = "resource";
    Mock mock = new Mock();
    doReturn(ImmutableList.of(resource)).when(mock.dataAccessor).getChildNames(argThat(new PropertyKeyArgument(PropertyType.IDEALSTATES)));
    doReturn(Collections.emptyList()).when(mock.dataAccessor).getChildNames(argThat(new PropertyKeyArgument(PropertyType.EXTERNALVIEW)));

    InstanceValidationUtil.siblingNodesActiveReplicaCheck(mock.dataAccessor, TEST_INSTANCE);
  }

  private class Mock {
    HelixDataAccessor dataAccessor;

    Mock() {
      this.dataAccessor = mock(HelixDataAccessor.class);
      when(dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
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
