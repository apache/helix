package org.apache.helix.zookeeper.routing;

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

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.exception.MultiZkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HTTP and ZK-based RoutingDataReader that first tries an HTTP call to MSDS and upon failure,
 * falls back to ZK for routing data.
 * HttpZkFallbackRoutingDataReader does not maintain a ZK connection - it establishes for reading
 * and closes it right away.
 */
public class HttpZkFallbackRoutingDataReader implements RoutingDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(HttpZkFallbackRoutingDataReader.class);

  /**
   * Returns a map form of metadata store routing data.
   * The map fields stand for metadata store realm address (key), and a corresponding list of ZK
   * path sharding keys (key).
   * @param endpoint two comma-separated endpoints. <HTTP endpoint>,<ZK address>
   * @return
   */
  @Override
  public Map<String, List<String>> getRawRoutingData(String endpoint) {
    Map<RoutingDataReaderType, String> endpointMap = parseEndpoint(endpoint);
    try {
      return new HttpRoutingDataReader()
          .getRawRoutingData(endpointMap.get(RoutingDataReaderType.HTTP));
    } catch (MultiZkException e) {
      LOG.warn(
          "HttpZkFallbackRoutingDataReader::getRawRoutingData: failed to read routing data via "
              + "HTTP. Falling back to ZK!", e);
      // TODO: increment failure count and emit as a metric
      return new ZkRoutingDataReader().getRawRoutingData(endpointMap.get(RoutingDataReaderType.ZK));
    }
  }

  /**
   * For a fallback routing data reader, endpoints are given in a comma-separated string in the
   * fallback order. This method parses the string and returns a map of routing data source type to
   * its respective endpoint.
   * @param endpointString
   * @return
   */
  private Map<RoutingDataReaderType, String> parseEndpoint(String endpointString) {
    String[] endpoints = endpointString.split(",");
    if (endpoints.length != 2) {
      throw new MultiZkException(
          "HttpZkFallbackRoutingDataReader::parseEndpoint: endpoint string does not contain two "
              + "proper comma-separated endpoints for HTTP and ZK! Endpoint string: "
              + endpointString);
    }
    return ImmutableMap
        .of(RoutingDataReaderType.HTTP, endpoints[0], RoutingDataReaderType.ZK, endpoints[1]);
  }
}
