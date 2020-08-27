package org.apache.helix.tools.ClusterVerifiers;

import java.util.Map;
import java.util.Set;

import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestBestPossibleExternalViewVerifier extends BestPossibleExternalViewVerifier {
  private static Logger LOG = LoggerFactory.getLogger(TestBestPossibleExternalViewVerifier.class);
  private static int COOL_DOWN = 2 * 1000;

  private TestBestPossibleExternalViewVerifier(RealmAwareZkClient zkClient, String clusterName,
      Map<String, Map<String, String>> errStates, Set<String> resources,
      Set<String> expectLiveInstances) {
    super (zkClient, clusterName, errStates, resources, expectLiveInstances);
  }
  /**
   * Deprecated - please use the Builder to construct this class.
   * @param zkAddr
   * @param clusterName
   * @param resources
   * @param errStates
   * @param expectLiveInstances
   */
  @Deprecated
  public TestBestPossibleExternalViewVerifier(String zkAddr, String clusterName, Set<String> resources,
      Map<String, Map<String, String>> errStates, Set<String> expectLiveInstances) {
    super(zkAddr, clusterName, resources, errStates, expectLiveInstances);
  }

  /**
   * Deprecated - please use the Builder to construct this class.
   * @param zkClient
   * @param clusterName
   * @param resources
   * @param errStates
   * @param expectLiveInstances
   */
  @Deprecated
  public TestBestPossibleExternalViewVerifier(RealmAwareZkClient zkClient, String clusterName,
      Set<String> resources, Map<String, Map<String, String>> errStates,
      Set<String> expectLiveInstances) {
    super(zkClient, clusterName, resources, errStates, expectLiveInstances);
  }

  public static class Builder extends ZkHelixClusterVerifier.Builder<TestBestPossibleExternalViewVerifier.Builder> {
    private final String _clusterName;
    private Map<String, Map<String, String>> _errStates;
    private Set<String> _resources;
    private Set<String> _expectLiveInstances;
    private RealmAwareZkClient _zkClient;

    public Builder(String clusterName) {
      _clusterName = clusterName;
    }

    public TestBestPossibleExternalViewVerifier build() {
      if (_clusterName == null) {
        throw new IllegalArgumentException("Cluster name is missing!");
      }

      if (_zkClient != null) {
        return new TestBestPossibleExternalViewVerifier(_zkClient, _clusterName, _resources, _errStates,
            _expectLiveInstances);
      }

      if (_realmAwareZkConnectionConfig == null || _realmAwareZkClientConfig == null) {
        // For backward-compatibility
        return new TestBestPossibleExternalViewVerifier(_zkAddress, _clusterName, _resources,
            _errStates, _expectLiveInstances);
      }

      validate();
      return new TestBestPossibleExternalViewVerifier(
          createZkClient(RealmAwareZkClient.RealmMode.SINGLE_REALM, _realmAwareZkConnectionConfig,
              _realmAwareZkClientConfig, _zkAddress), _clusterName, _errStates, _resources,
          _expectLiveInstances);
    }

    public String getClusterName() {
      return _clusterName;
    }

    public Map<String, Map<String, String>> getErrStates() {
      return _errStates;
    }

    public TestBestPossibleExternalViewVerifier.Builder setErrStates(Map<String, Map<String, String>> errStates) {
      _errStates = errStates;
      return this;
    }

    public Set<String> getResources() {
      return _resources;
    }

    public TestBestPossibleExternalViewVerifier.Builder setResources(Set<String> resources) {
      _resources = resources;
      return this;
    }

    public Set<String> getExpectLiveInstances() {
      return _expectLiveInstances;
    }

    public TestBestPossibleExternalViewVerifier.Builder setExpectLiveInstances(Set<String> expectLiveInstances) {
      _expectLiveInstances = expectLiveInstances;
      return this;
    }

    public String getZkAddr() {
      return _zkAddress;
    }

    public TestBestPossibleExternalViewVerifier.Builder setZkClient(RealmAwareZkClient zkClient) {
      _zkClient = zkClient;
      return this;
    }
  }

  @Override
  public boolean verifyByPolling(long timeout, long period) {
    try {
      Thread.sleep(COOL_DOWN);
    } catch (InterruptedException e) {
      LOG.error("sleeping in verifyByPolling interrupted");
    }
    return super.verifyByPolling(timeout, period);
  }
}
