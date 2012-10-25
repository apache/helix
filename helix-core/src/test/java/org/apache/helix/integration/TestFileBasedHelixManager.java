package org.apache.helix.integration;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.helix.ClusterView;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.file.StaticFileHelixManager;
import org.apache.helix.tools.ClusterViewSerializer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


// TODO remove this test
public class TestFileBasedHelixManager
{
  @Test ()
  public void testFileBasedHelixManager() throws Exception
  {
    List<StaticFileHelixManager.DBParam> dbParams = new ArrayList<StaticFileHelixManager.DBParam>();
    dbParams.add(new StaticFileHelixManager.DBParam("BizFollow", 1));
    dbParams.add(new StaticFileHelixManager.DBParam("BizProfile", 1));
    dbParams.add(new StaticFileHelixManager.DBParam("EspressoDB", 10));
    dbParams.add(new StaticFileHelixManager.DBParam("MailboxDB", 128));
    dbParams.add(new StaticFileHelixManager.DBParam("MyDB", 8));
    dbParams.add(new StaticFileHelixManager.DBParam("schemata", 1));
    String[] nodesInfo =
    { "localhost:8900", "localhost:8901", "localhost:8902", "localhost:8903",
        "localhost:8904" };
    
    String file = "/tmp/clusterView.json";
    int replica = 0;
    // ClusterViewSerializer serializer = new ClusterViewSerializer(file);
    ClusterView view = StaticFileHelixManager.generateStaticConfigClusterView(nodesInfo, dbParams, replica);
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
