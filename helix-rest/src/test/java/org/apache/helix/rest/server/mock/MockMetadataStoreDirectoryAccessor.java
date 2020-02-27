package org.apache.helix.rest.server.mock;

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

import javax.ws.rs.Path;

import org.apache.helix.msdcommon.datamodel.TrieRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.metadatastore.MetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.ZkMetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.accessor.MetadataStoreRoutingDataReader;
import org.apache.helix.rest.metadatastore.accessor.MetadataStoreRoutingDataWriter;
import org.apache.helix.rest.metadatastore.accessor.ZkRoutingDataReader;
import org.apache.helix.rest.metadatastore.accessor.ZkRoutingDataWriter;
import org.apache.helix.rest.server.resources.metadatastore.MetadataStoreDirectoryAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An accessor that mocks the MetadataStoreDirectoryAccessor for testing purpose.
 */
@Path("/mock")
public class MockMetadataStoreDirectoryAccessor extends MetadataStoreDirectoryAccessor {
  //TODO: use this class as a template for https://github.com/apache/helix/issues/816

  private static final Logger LOG =
      LoggerFactory.getLogger(MockMetadataStoreDirectoryAccessor.class);
  // A flag that will be modified if the underlying MockZkRoutingDataWriter makes an operation
  // against ZooKeeper
  public static boolean operatedOnZk = false;
  // The instance of mockMSD that's created by this accessor; it's saved here to be closed later
  public static MetadataStoreDirectory _mockMSDInstance;

  /**
   * This method is overriden so that an instance of MockZkMetadataStoreDirectory can be passed in
   */
  @Override
  protected void buildMetadataStoreDirectory(String namespace, String address) {
    try {
      _metadataStoreDirectory = new MockZkMetadataStoreDirectory(namespace, address);
      _mockMSDInstance = _metadataStoreDirectory;
    } catch (InvalidRoutingDataException e) {
      LOG.error("buildMetadataStoreDirectory encountered an exception.", e);
    }
  }

  /**
   * Used to artificially create another instance of ZkMetadataStoreDirectory.
   * ZkMetadataStoreDirectory being a singleton makes it difficult to test it,
   * therefore this is the only way to create another instance.
   */
  class MockZkMetadataStoreDirectory extends ZkMetadataStoreDirectory {
    MockZkMetadataStoreDirectory(String namespace, String zkAddress)
        throws InvalidRoutingDataException {
      super();

      // Manually populate the map so that MockZkRoutingDataWriter can be passed in
      _routingZkAddressMap.put(namespace, zkAddress);
      _routingDataReaderMap.put(namespace, new ZkRoutingDataReader(namespace, zkAddress, this));
      _routingDataWriterMap.put(namespace, new MockZkRoutingDataWriter(namespace, zkAddress));
      _realmToShardingKeysMap.put(namespace, _routingDataReaderMap.get(namespace).getRoutingData());
      _routingDataMap.put(namespace, new TrieRoutingData(_realmToShardingKeysMap.get(namespace)));
    }

    @Override
    public void close() {
      _routingDataReaderMap.values().forEach(MetadataStoreRoutingDataReader::close);
      _routingDataWriterMap.values().forEach(MetadataStoreRoutingDataWriter::close);
    }
  }

  /**
   * A mock to ZkRoutingDataWriter. The only purpose is to set the static flag signifying that
   * this writer is used for zookeeper operations.
   */
  class MockZkRoutingDataWriter extends ZkRoutingDataWriter {
    public MockZkRoutingDataWriter(String namespace, String zkAddress) {
      super(namespace, zkAddress);
      operatedOnZk = false;
    }

    @Override
    protected boolean createZkRealm(String realm) {
      operatedOnZk = true;
      return super.createZkRealm(realm);
    }

    @Override
    protected boolean deleteZkRealm(String realm) {
      operatedOnZk = true;
      return super.deleteZkRealm(realm);
    }

    @Override
    protected boolean createZkShardingKey(String realm, String shardingKey) {
      operatedOnZk = true;
      return super.createZkShardingKey(realm, shardingKey);
    }

    @Override
    protected boolean deleteZkShardingKey(String realm, String shardingKey) {
      operatedOnZk = true;
      return super.deleteZkShardingKey(realm, shardingKey);
    }
  }
}
