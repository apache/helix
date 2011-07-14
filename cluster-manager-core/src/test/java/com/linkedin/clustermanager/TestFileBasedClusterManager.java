package com.linkedin.clustermanager;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.file.FileBasedClusterManager;
import com.linkedin.clustermanager.tools.ClusterViewSerializer;

public class TestFileBasedClusterManager
{
  @Test
  public void testInvocation() throws Exception
  {
    List<FileBasedClusterManager.DBParam> dbParams = new ArrayList<FileBasedClusterManager.DBParam>();
    dbParams.add(new FileBasedClusterManager.DBParam("BizFollow", 1));
    dbParams.add(new FileBasedClusterManager.DBParam("BizProfile", 1));
    dbParams.add(new FileBasedClusterManager.DBParam("EspressoDB", 10));
    dbParams.add(new FileBasedClusterManager.DBParam("MailboxDB", 128));
    dbParams.add(new FileBasedClusterManager.DBParam("MyDB", 8));
    dbParams.add(new FileBasedClusterManager.DBParam("schemata", 1));
    String[] nodesInfo =
    { "localhost:8900", "localhost:8901", "localhost:8902", "localhost:8903",
        "localhost:8904" };
    
    String file = "/tmp/cluster-12345.cv";
    ClusterViewSerializer serializer = new ClusterViewSerializer(file);
    ClusterView view = FileBasedClusterManager.generateStaticConfigClusterView(nodesInfo, dbParams, 0);
    view.setExternalView(new LinkedList<ZNRecord>());
    byte[] bytes;
    bytes = serializer.serialize(view);
    ClusterView restoredView = (ClusterView) serializer.deserialize(bytes);
    
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
