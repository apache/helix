/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.ClusterView;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.file.StaticFileHelixManager;
import com.linkedin.helix.tools.ClusterViewSerializer;

public class TestClusterviewSerializer
{
  @Test ()
  public void testClusterviewSerializer() throws Exception
  {
    List<StaticFileHelixManager.DBParam> dbParams = new ArrayList<StaticFileHelixManager.DBParam>();
    // dbParams.add(new FileBasedClusterManager.DBParam("BizFollow", 1));
    dbParams.add(new StaticFileHelixManager.DBParam("BizProfile", 1));
    // dbParams.add(new FileBasedClusterManager.DBParam("EspressoDB", 10));
    // dbParams.add(new FileBasedClusterManager.DBParam("MailboxDB", 128));
    // dbParams.add(new FileBasedClusterManager.DBParam("MyDB", 8));
    // dbParams.add(new FileBasedClusterManager.DBParam("schemata", 1));
    // String[] nodesInfo = { "localhost:8900", "localhost:8901",
    // "localhost:8902", "localhost:8903",
    // "localhost:8904" };
    String[] nodesInfo = { "localhost:12918" };
    int replication = 0;

    ClusterView view = StaticFileHelixManager.generateStaticConfigClusterView(nodesInfo, dbParams, replication);
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
    AssertJUnit.assertEquals(view1.getPropertyLists().size(), view2.getPropertyLists().size());
    AssertJUnit.assertEquals(view1.getExternalView().size(), view2.getExternalView().size());
    AssertJUnit.assertEquals(view1.getMemberInstanceMap().size(), view2.getMemberInstanceMap().size());
    AssertJUnit.assertEquals(view1.getInstances().size(), view2.getInstances().size());
  }

}
