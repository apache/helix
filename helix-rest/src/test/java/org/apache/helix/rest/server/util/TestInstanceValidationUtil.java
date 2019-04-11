package org.apache.helix.rest.server.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.InstanceValidationUtil;
import org.junit.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class TestInstanceValidationUtil{
  private static final String RESOURCE_NAME = "TestResource";
  private static final String TEST_CLUSTER = "TestCluster";

  @Test
  public void testPartitionLevelCheck() {
    List<ExternalView> externalViews = new ArrayList<>(Arrays.asList(prepareExternalView()));
    Mock mock = new Mock();
    HelixDataAccessor accessor = mock.dataAccessor;

    when(mock.dataAccessor.keyBuilder())
        .thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
    when(mock.dataAccessor
        .getProperty(new PropertyKey.Builder(TEST_CLUSTER).stateModelDef(MasterSlaveSMD.name)))
        .thenReturn(mock.stateModel);
    when(mock.stateModel.getTopState()).thenReturn("MASTER");
    List<String> failedPartitions = InstanceValidationUtil
        .perPartitionHealthCheck(externalViews, preparePartitionStateMap(), "h2", accessor);

    Assert.assertTrue(failedPartitions.size() == 1);
    Assert.assertEquals(failedPartitions.iterator().next(), "p2");
  }

  private ExternalView prepareExternalView() {
    ExternalView externalView = new ExternalView(RESOURCE_NAME);
    externalView.getRecord()
        .setSimpleField(ExternalView.ExternalViewProperty.STATE_MODEL_DEF_REF.toString(),
            MasterSlaveSMD.name);
    externalView.setState("p1", "h1", "MASTER");
    externalView.setState("p1", "h2", "SLAVE");
    externalView.setState("p1", "h3", "SLAVE");

    externalView.setState("p2", "h1", "SLAVE");
    externalView.setState("p2", "h2", "MASTER");
    externalView.setState("p2", "h3", "SLAVE");

    externalView.setState("p3", "h1", "SLAVE");
    externalView.setState("p3", "h2", "MASTER");
    externalView.setState("p3", "h3", "SLAVE");

    return externalView;
  }

  private Map<String, Map<String, Boolean>> preparePartitionStateMap() {
    Map<String, Map<String, Boolean>> partitionStateMap = new HashMap<>();
    partitionStateMap.put("h1", new HashMap<>());
    partitionStateMap.put("h2", new HashMap<>());
    partitionStateMap.put("h3", new HashMap<>());

    // h1 holds master for p1 is unhealthy should not impact decision of shut down h2
    // But h2 holds master for p2, shutdown h2 may cause unhealthy master on h3.
    partitionStateMap.get("h1").put("p1", false);
    partitionStateMap.get("h1").put("p2", true);
    partitionStateMap.get("h1").put("p3", true);

    partitionStateMap.get("h2").put("p1", true);
    partitionStateMap.get("h2").put("p2", true);
    partitionStateMap.get("h2").put("p3", true);

    partitionStateMap.get("h3").put("p1", true);
    partitionStateMap.get("h3").put("p2", false);
    partitionStateMap.get("h3").put("p3", true);

    return partitionStateMap;
  }

  private final class Mock {
    private HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    private StateModelDefinition stateModel = mock(StateModelDefinition.class);

    Mock() {
    }
  }
}
