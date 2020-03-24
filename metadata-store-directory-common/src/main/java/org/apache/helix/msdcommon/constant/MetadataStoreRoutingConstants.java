package org.apache.helix.msdcommon.constant;

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

public class MetadataStoreRoutingConstants {
  public static final String ROUTING_DATA_PATH = "/METADATA_STORE_ROUTING_DATA";

  public static final String ROUTING_ZK_ADDRESS_KEY = "ROUTING_ZK_ADDRESS";

  // For ZK only
  public static final String ZNRECORD_LIST_FIELD_KEY = "ZK_PATH_SHARDING_KEYS";

  // Leader election ZNode for ZkRoutingDataWriter
  public static final String LEADER_ELECTION_ZNODE = "/_ZK_ROUTING_DATA_WRITER_LEADER";

  /** Field name in JSON REST response of getting all metadata store namespaces. */
  public static final String METADATA_STORE_NAMESPACES = "namespaces";

  /** Field name in JSON REST response of getting all sharding keys in a single namespace. */
  public static final String SINGLE_METADATA_STORE_NAMESPACE = "namespace";

  /** Field name in JSON REST response of getting metadata store realms in one namespace. */
  public static final String METADATA_STORE_REALMS = "realms";

  /** Field name in JSON REST response of getting sharding keys in one realm. */
  public static final String SINGLE_METADATA_STORE_REALM = "realm";

  /** Field name in JSON REST response of getting sharding keys. */
  public static final String SHARDING_KEYS = "shardingKeys";

  /** Field name in JSON REST response of getting routing data. */
  public static final String ROUTING_DATA = "routingData";

  /** Field name in JSON REST response related to one single sharding key. */
  public static final String SINGLE_SHARDING_KEY = "shardingKey";

  /**
   * Field name in JSON response of the REST endpoint getting sharding keys with prefix:
   * "GET /sharding-keys?prefix={prefix}"
   * It is used in below response as an example:
   * {
   * 	"prefix": "/sharding/key",
   * 	"shardingKeys": [{
   * 		"realm": "testRealm2",
   * 		"shardingKey": "/sharding/key/1/f"
   *  }]
   * }
   */
  public static final String SHARDING_KEY_PATH_PREFIX = "prefix";

  // System Property Metadata Store Directory Server endpoint key
  public static final String MSDS_SERVER_ENDPOINT_KEY = "metadataStoreDirectoryServerEndpoint";

  // Prefix to MSDS resource endpoints
  public static final String MSDS_NAMESPACES_URL_PREFIX = "/namespaces";

  // MSDS resource getAllRealms endpoint string
  public static final String MSDS_GET_ALL_REALMS_ENDPOINT = "/metadata-store-realms";

  // MSDS resource get all routing data endpoint string
  public static final String MSDS_GET_ALL_ROUTING_DATA_ENDPOINT = "/routing-data";

  // MSDS resource get all sharding keys endpoint string
  public static final String MSDS_GET_ALL_SHARDING_KEYS_ENDPOINT = "/sharding-keys";

  // The key for system properties that contains the hostname of the
  // MetadataStoreDirectoryService server instance
  public static final String MSDS_SERVER_HOSTNAME_KEY = "msds_hostname";

  // The key for system properties that contains the port of the
  // MetadataStoreDirectoryService server instance
  public static final String MSDS_SERVER_PORT_KEY = "msds_port";

  // This is added for helix-rest 2.0. For example, without this value, the url will be
  // "localhost:9998"; with this value, the url will be "localhost:9998/admin/v2" if this
  // value is "/admin/v2".
  public static final String MSDS_CONTEXT_URL_PREFIX_KEY = "msds_context_url_prefix";
}
