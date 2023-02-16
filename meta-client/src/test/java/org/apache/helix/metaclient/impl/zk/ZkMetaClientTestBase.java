package org.apache.helix.metaclient.impl.zk;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.zkclient.IDefaultNameSpace;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public abstract class ZkMetaClientTestBase {

  // A Serializer for test. transfer byte <-> String
  public static class TestStringSerializer implements PathBasedZkSerializer {

    @Override
    public byte[] serialize(Object data, String path) throws ZkMarshallingError {
      return ((String) data).getBytes();
    }

    @Override
    public Object deserialize(byte[] bytes, String path) throws ZkMarshallingError {
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  protected static final String ZK_ADDR = "localhost:2183";
  protected static final int DEFAULT_TIMEOUT_MS = 1000;
  protected static final String ENTRY_STRING_VALUE = "test-value";

  private final Object _syncObject = new Object();

  private ZkServer _zkServer;

  @BeforeClass
  public void prepare() {
    // start local zookeeper server
    _zkServer = startZkServer(ZK_ADDR);
  }

  @AfterClass
  public void cleanUp() {
    _zkServer.shutdown();
  }

  protected static ZkMetaClient<String> createZkMetaClient() {
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR)
            //.setZkSerializer(new TestStringSerializer())
            .build();
    return new ZkMetaClient<>(config);
  }

  protected static ZkServer startZkServer(final String zkAddress) {
    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";

    // Clean up local directory
    try {
      FileUtils.deleteDirectory(new File(dataDir));
      FileUtils.deleteDirectory(new File(logDir));
    } catch (IOException e) {
      e.printStackTrace();
    }

    IDefaultNameSpace defaultNameSpace = zkClient -> {
    };

    int port = Integer.parseInt(zkAddress.substring(zkAddress.lastIndexOf(':') + 1));
    System.out.println("Starting ZK server at " + zkAddress);
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();
    return zkServer;
  }



}
