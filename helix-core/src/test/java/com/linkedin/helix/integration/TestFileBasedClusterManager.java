package com.linkedin.helix.integration;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.ClusterView;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.agent.file.StaticFileClusterManager;
import com.linkedin.helix.tools.ClusterViewSerializer;

// TODO remove this test
public class TestFileBasedClusterManager
{
  @Test ()
  public void testFileBasedClusterManager() throws Exception
  {
    List<StaticFileClusterManager.DBParam> dbParams = new ArrayList<StaticFileClusterManager.DBParam>();
    dbParams.add(new StaticFileClusterManager.DBParam("BizFollow", 1));
    dbParams.add(new StaticFileClusterManager.DBParam("BizProfile", 1));
    dbParams.add(new StaticFileClusterManager.DBParam("EspressoDB", 10));
    dbParams.add(new StaticFileClusterManager.DBParam("MailboxDB", 128));
    dbParams.add(new StaticFileClusterManager.DBParam("MyDB", 8));
    dbParams.add(new StaticFileClusterManager.DBParam("schemata", 1));
    String[] nodesInfo =
    { "localhost:8900", "localhost:8901", "localhost:8902", "localhost:8903",
        "localhost:8904" };
    
    String file = "/tmp/clusterView.json";
    int replica = 0;
    // ClusterViewSerializer serializer = new ClusterViewSerializer(file);
    ClusterView view = StaticFileClusterManager.generateStaticConfigClusterView(nodesInfo, dbParams, replica);
    view.setExternalView(new LinkedList<ZNRecord>());
    ClusterViewSerializer.serialize(view, new File(file));
    ClusterView restoredView = ClusterViewSerializer.deserialize(new File(file));
    
    verifyClusterViews(view, restoredView);
  }
  
  public void verifyClusterViews(ClusterView view1, ClusterView view2)
  {
    AssertJUnit.assertEquals(view1.getPropertyLists().size(), view2.getPropertyLists().size());
    AssertJUnit.assertEquals(view1.getExternalView().size(), view2.getExternalView().size());
    AssertJUnit.assertEquals(view1.getMemberInstanceMap().size(), view2.getMemberInstanceMap().size());
    AssertJUnit.assertEquals(view1.getInstances().size(), view2.getInstances().size());
  }
    
}
