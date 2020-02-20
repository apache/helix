package org.apache.helix.rest.metadatastore.accessor;

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

import java.util.List;
import java.util.Map;

import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;


/**
 * An interface for a DAO that fetches routing data from a source and return a key-value mapping
 * that represent the said routing data.
 * Note: Each data reader connects to a single namespace.
 */
public interface MetadataStoreRoutingDataReader {

  /**
   * Fetches routing data from the data source.
   * @return a mapping from "metadata store realm addresses" to lists of "metadata store sharding
   *         keys", where the sharding keys in a value list all route to the realm address in the
   *         key
   * @throws InvalidRoutingDataException - when the routing data is malformed in any way that
   *           disallows a meaningful mapping to be returned
   */
  Map<String, List<String>> getRoutingData()
      throws InvalidRoutingDataException;

  /**
   * Closes any stateful resources such as connections or threads.
   */
  void close();
}
