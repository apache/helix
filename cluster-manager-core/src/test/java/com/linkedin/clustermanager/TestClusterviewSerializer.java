package com.linkedin.clustermanager;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.file.FileBasedClusterManager;
import com.linkedin.clustermanager.tools.ClusterViewSerializer;

@Test (groups = {"unitTest"})
public class TestClusterviewSerializer
{
  @Test
  public void testClusterviewSerializer() throws Exception
  {
    List<FileBasedClusterManager.DBParam> dbParams = new ArrayList<FileBasedClusterManager.DBParam>();
    // dbParams.add(new FileBasedClusterManager.DBParam("BizFollow", 1));
    dbParams.add(new FileBasedClusterManager.DBParam("BizProfile", 1));
    // dbParams.add(new FileBasedClusterManager.DBParam("EspressoDB", 10));
    // dbParams.add(new FileBasedClusterManager.DBParam("MailboxDB", 128));
    // dbParams.add(new FileBasedClusterManager.DBParam("MyDB", 8));
    // dbParams.add(new FileBasedClusterManager.DBParam("schemata", 1));
    // String[] nodesInfo = { "localhost:8900", "localhost:8901",
    // "localhost:8902", "localhost:8903",
    // "localhost:8904" };
    String[] nodesInfo = { "localhost:12918" };
    int replication = 0;

    ClusterView view = FileBasedClusterManager.generateStaticConfigClusterView(nodesInfo, dbParams, replication);
    view.setExternalView(new LinkedList<ZNRecord>());
    String file = "/tmp/clusterView.json";
    // ClusterViewSerializer serializer = new ClusterViewSerializer(file);

    // byte[] bytes;
    ClusterViewSerializer.serialize(view, new File(file));
    // String str1 = new String(bytes);
    ClusterView restoredView = ClusterViewSerializer.deserialize(new File(file));
    // logger.info(restoredView);

    // byte[] bytes2 = serializer.serialize(restoredView);
    
    VerifyClusterViews(view, restoredView);
  }
  
  public void VerifyClusterViews(ClusterView view1, ClusterView view2)
  {
    AssertJUnit.assertEquals(view1.getClusterPropertyLists().size(), view2.getClusterPropertyLists().size());
    AssertJUnit.assertEquals(view1.getExternalView().size(), view2.getExternalView().size());
    AssertJUnit.assertEquals(view1.getMemberInstanceMap().size(), view2.getMemberInstanceMap().size());
    AssertJUnit.assertEquals(view1.getInstances().size(), view2.getInstances().size());
  }
    
}
