package org.apache.helix.rest.server.resources.metadatastore;

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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.HelixRestUtils;
import org.apache.helix.rest.metadatastore.MetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.ZkMetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.datamodel.MetadataStoreShardingKey;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides REST endpoints for accessing metadata store directory service,
 * which responds to read/write requests of metadata store realms, sharding keys, etc..
 */
@Path("")
public class MetadataStoreDirectoryAccessor extends AbstractResource {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataStoreDirectoryAccessor.class);

  private String _namespace;
  private MetadataStoreDirectory _metadataStoreDirectory;

  @PostConstruct
  private void postConstruct() {
    HelixRestNamespace helixRestNamespace = getHelixNamespace();
    _namespace = helixRestNamespace.getName();

    buildMetadataStoreDirectory(_namespace, helixRestNamespace.getMetadataStoreAddress());
  }

  @PreDestroy
  private void preDestroy() {
    _metadataStoreDirectory.close();
  }

  /**
   * Gets all existing namespaces in the routing metadata store at endpoint:
   * "GET /metadata-store-namespaces"
   *
   * @return Json response of all namespaces.
   */
  @GET
  @Path("/metadata-store-namespaces")
  public Response getAllNamespaces() {
    Collection<String> namespaces = _metadataStoreDirectory.getAllNamespaces();
    Map<String, Collection<String>> responseMap =
        ImmutableMap.of(MetadataStoreRoutingConstants.METADATA_STORE_NAMESPACES, namespaces);

    return JSONRepresentation(responseMap);
  }

  /**
   * Gets all metadata store realms in a namespace at path: "GET /metadata-store-realms",
   * or gets a metadata store realm with the sharding key at path:
   * "GET /metadata-store-realms?sharding-key={sharding-key}"
   *
   * @return Json representation of all realms.
   */
  @GET
  @Path("/metadata-store-realms")
  public Response getAllMetadataStoreRealms(@QueryParam("sharding-key") String shardingKey) {
    try {
      if (shardingKey == null) {
        // Get all realms: "GET /metadata-store-realms"
        Collection<String> realms = _metadataStoreDirectory.getAllMetadataStoreRealms(_namespace);
        Map<String, Collection<String>> responseMap =
            ImmutableMap.of(MetadataStoreRoutingConstants.METADATA_STORE_REALMS, realms);
        return JSONRepresentation(responseMap);
      }

      // Get a single realm filtered by sharding key:
      // "GET /metadata-store-realms?sharding-key={sharding-key}"
      String realm = _metadataStoreDirectory.getMetadataStoreRealm(_namespace, shardingKey);
      return JSONRepresentation(new MetadataStoreShardingKey(shardingKey, realm));
    } catch (NoSuchElementException ex) {
      return notFound(ex.getMessage());
    }
  }

  @PUT
  @Path("/metadata-store-realms/{realm}")
  public Response addMetadataStoreRealm(@PathParam("realm") String realm) {
    try {
      _metadataStoreDirectory.addMetadataStoreRealm(_namespace, realm);
    } catch (IllegalArgumentException ex) {
      return notFound(ex.getMessage());
    }

    return created();
  }

  @DELETE
  @Path("/metadata-store-realms/{realm}")
  public Response deleteMetadataStoreRealm(@PathParam("realm") String realm) {
    try {
      _metadataStoreDirectory.deleteMetadataStoreRealm(_namespace, realm);
    } catch (IllegalArgumentException ex) {
      return notFound(ex.getMessage());
    }

    return OK();
  }

  /**
   * Gets all sharding keys for following requests:
   * - "HTTP GET /sharding-keys" which returns all sharding keys in a namespace.
   * - "HTTP GET /sharding-keys?prefix={prefix}" which returns sharding keys that have the prefix.
   * -- JSON response example for this path:
   * {
   * 	"prefix": "/sharding/key",
   * 	"shardingKeys": [{
   * 		"realm": "testRealm2",
   * 		"shardingKey": "/sharding/key/1/f"
   *    }, {
   * 		"realm": "testRealm2",
   * 		"shardingKey": "/sharding/key/1/e"
   *  }, {
   * 		"realm": "testRealm1",
   * 		"shardingKey": "/sharding/key/1/b"
   *  }, {
   * 		"realm": "testRealm1",
   * 		"shardingKey": "/sharding/key/1/a"
   *  }]
   * }
   *
   * @param prefix Query param in endpoint path: prefix substring of sharding key.
   * @return Json representation for the sharding keys.
   */
  @GET
  @Path("/sharding-keys")
  public Response getShardingKeys(@QueryParam("prefix") String prefix) {
    try {
      if (prefix == null) {
        // For endpoint: "/sharding-keys" to get all sharding keys in a namespace.
        return getAllShardingKeys();
      }
      // For endpoint: "/sharding-keys?prefix={prefix}"
      return getAllShardingKeysUnderPath(prefix);
    } catch (NoSuchElementException ex) {
      return notFound(ex.getMessage());
    }
  }

  /**
   * Gets all path-based sharding keys for a queried realm at endpoint:
   * "GET /metadata-store-realms/{realm}/sharding-keys"
   * <p>
   * "GET /metadata-store-realms/{realm}/sharding-keys?prefix={prefix}" is also supported,
   * which is helpful when you want to check what sharding keys have the prefix substring.
   *
   * @param realm Queried metadata store realm to get sharding keys.
   * @param prefix Query param in endpoint path: prefix substring of sharding key.
   * @return All path-based sharding keys in the queried realm.
   */
  @GET
  @Path("/metadata-store-realms/{realm}/sharding-keys")
  public Response getRealmShardingKeys(@PathParam("realm") String realm,
      @QueryParam("prefix") String prefix) {
    try {
      if (prefix == null) {
        return getAllShardingKeysInRealm(realm);
      }

      // For "GET /metadata-store-realms/{realm}/sharding-keys?prefix={prefix}"
      return getRealmShardingKeysUnderPath(realm, prefix);
    } catch (NoSuchElementException ex) {
      return notFound(ex.getMessage());
    }
  }

  @PUT
  @Path("/metadata-store-realms/{realm}/sharding-keys/{sharding-key: .+}")
  public Response addShardingKey(@PathParam("realm") String realm,
      @PathParam("sharding-key") String shardingKey) {
    try {
      _metadataStoreDirectory.addShardingKey(_namespace, realm, shardingKey);
    } catch (IllegalArgumentException ex) {
      return notFound(ex.getMessage());
    }

    return created();
  }

  @DELETE
  @Path("/metadata-store-realms/{realm}/sharding-keys/{sharding-key: .+}")
  public Response deleteShardingKey(@PathParam("realm") String realm,
      @PathParam("sharding-key") String shardingKey) {
    try {
      _metadataStoreDirectory.deleteShardingKey(_namespace, realm, shardingKey);
    } catch (IllegalArgumentException ex) {
      return notFound(ex.getMessage());
    }

    return OK();
  }

  private HelixRestNamespace getHelixNamespace() {
    HelixRestNamespace helixRestNamespace = null;
    // A default servlet does not have context property key METADATA, so the namespace
    // is retrieved from property ALL_NAMESPACES.
    if (HelixRestUtils.isDefaultServlet(_servletRequest.getServletPath())) {
      // It is safe to ignore uncheck warnings for this cast.
      @SuppressWarnings("unchecked")
      List<HelixRestNamespace> namespaces = (List<HelixRestNamespace>) _application.getProperties()
          .get(ContextPropertyKeys.ALL_NAMESPACES.name());
      for (HelixRestNamespace ns : namespaces) {
        if (HelixRestNamespace.DEFAULT_NAMESPACE_NAME.equals(ns.getName())) {
          helixRestNamespace = ns;
          break;
        }
      }
    } else {
      // Get namespace from property METADATA for a common servlet.
      helixRestNamespace = (HelixRestNamespace) _application.getProperties()
          .get(ContextPropertyKeys.METADATA.name());
    }

    return helixRestNamespace;
  }

  private void buildMetadataStoreDirectory(String namespace, String address) {
    Map<String, String> routingZkAddressMap = ImmutableMap.of(namespace, address);
    try {
      _metadataStoreDirectory = new ZkMetadataStoreDirectory(routingZkAddressMap);
    } catch (InvalidRoutingDataException ex) {
      // In this case, the InvalidRoutingDataException should not happen because routing
      // ZK address is always valid here.
      LOG.warn("Unable to create metadata store directory for routing ZK address: {}",
          routingZkAddressMap, ex);
    }
  }

  private Response getAllShardingKeys() {
    Collection<String> shardingKeys = _metadataStoreDirectory.getAllShardingKeys(_namespace);
    Map<String, Object> responseMap = ImmutableMap
        .of(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_NAMESPACE, _namespace,
            MetadataStoreRoutingConstants.SHARDING_KEYS, shardingKeys);

    return JSONRepresentation(responseMap);
  }

  private Response getAllShardingKeysInRealm(String realm) {
    Collection<String> shardingKeys =
        _metadataStoreDirectory.getAllShardingKeysInRealm(_namespace, realm);

    Map<String, Object> responseMap = ImmutableMap
        .of(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM, realm,
            MetadataStoreRoutingConstants.SHARDING_KEYS, shardingKeys);

    return JSONRepresentation(responseMap);
  }

  private Response getAllShardingKeysUnderPath(String prefix) {
    List<MetadataStoreShardingKey> shardingKeyList =
        _metadataStoreDirectory.getAllMappingUnderPath(_namespace, prefix).entrySet().stream()
            .map(entry -> new MetadataStoreShardingKey(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());

    Map<String, Object> responseMap = ImmutableMap
        .of(MetadataStoreRoutingConstants.SHARDING_KEY_PATH_PREFIX, prefix,
            MetadataStoreRoutingConstants.SHARDING_KEYS, shardingKeyList);

    return JSONRepresentation(responseMap);
  }

  private Response getRealmShardingKeysUnderPath(String realm, String prefix) {
    List<String> shardingKeyList =
        _metadataStoreDirectory.getAllMappingUnderPath(_namespace, prefix).entrySet().stream()
            .filter(entry -> entry.getValue().equals(realm)).map(Map.Entry::getKey)
            .collect(Collectors.toList());

    Map<String, Object> responseMap = ImmutableMap
        .of(MetadataStoreRoutingConstants.SHARDING_KEY_PATH_PREFIX, prefix,
            MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM, realm,
            MetadataStoreRoutingConstants.SHARDING_KEYS, shardingKeyList);

    return JSONRepresentation(responseMap);
  }
}
