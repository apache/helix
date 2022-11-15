package org.apache.helix.integration.multizk;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.*;

/**
 * This class test multi implementation in FederatedZkClient. Extends MultiZkTestBase as the test require a multi zk
 * server setup.
 */
public class TestMultiInMultiZk extends MultiZkTestBase {

    @BeforeClass
    public void beforeClass() throws Exception {
        super.beforeClass();
        // Routing data may be set by other tests using the same endpoint; reset() for good measure
        RoutingDataManager.getInstance().reset();
        // Create a FederatedZkClient for admin work

        try {
            _zkClient =
                    new FederatedZkClient(new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
                            .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
                            .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name()).build(),
                            new RealmAwareZkClient.RealmAwareZkClientConfig());
            _zkClient.setZkSerializer(new ZNRecordSerializer());
        } catch (Exception ex) {
            for (StackTraceElement elm : ex.getStackTrace()) {
                System.out.println(elm);
            }
        }
        System.out.println("end start");
    }

    @AfterClass
    public void afterClass() throws Exception {
        super.afterClass();
    }

    /**
     * Calling multi on op of different realms/servers.
     * Should fail.
     */
    @Test
    public void testMultiDiffRealm() {
        List<Op> ops = Arrays.asList(
                Op.create(CLUSTER_LIST.get(0), new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(CLUSTER_LIST.get(1), new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(CLUSTER_LIST.get(2), new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(CLUSTER_LIST.get(0) + "/test", new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

        try {
            //Execute transactional support on operations and verify they were run
            _zkClient.multi(ops);
            Assert.fail("Should have thrown an exception. Cannot run multi on ops of different servers.");
        } catch (IllegalArgumentException e) {
            boolean pathExists = _zkClient.exists("/" + CLUSTER_LIST.get(0) + "/test");
            Assert.assertFalse(pathExists, "Path should not have been created.");
        }
    }
}
