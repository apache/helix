package org.apache.helix.gateway.util;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PollChannelUtil {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger logger = LoggerFactory.getLogger(PollChannelUtil.class);

  // return  <grpc health stub, ManagedChannel> pair
  // ManagedChannel need to be shutdown when the connection is no longer needed
  public static Pair<HealthGrpc.HealthBlockingStub, ManagedChannel> createGrpcChannel(String endpointPortString) {
    String[] endpointPort = endpointPortString.split(":");
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(endpointPort[0], Integer.parseInt(endpointPort[1])).usePlaintext().build();

    return new ImmutablePair<>(HealthGrpc.newBlockingStub(channel), channel);
  }

  /**
   * Send Unary RPC to the gRPC service to check the health of the container. Could be liveness or readiness depends on input.
   * @param service one of "readiness" or "liveness"
   *                https://github.com/kubernetes/kubernetes/issues/115651
   * @return
   */
  public static boolean fetchLivenessStatusFromGrpcService(String service, HealthGrpc.HealthBlockingStub healthStub) {
    HealthCheckRequest request = HealthCheckRequest.newBuilder().setService(service).build();
    HealthCheckResponse response = healthStub.check(request);
    return response.getStatus() == HealthCheckResponse.ServingStatus.SERVING;
  }

  /**
   * Flush the current assignment map to a file. The whole file is re-written every time.
   */
  public static void flushAssignmentToFile(String targetAssignment, String filePath) {
    try (FileWriter fileWriter = new FileWriter(filePath)) {
      fileWriter.write(targetAssignment);
      fileWriter.close();
    } catch (IOException e) {
      logger.warn("Failed to write to file: " + filePath, e);
    }
  }

  /**
   * read current state from a file, compare with in memory current state, update the in memory current state and return diff.
   * Current state file format: {"cluster1" : { "instance_1" : { "resource1" : {"shard1” : “online" }}}}
   */
  public static Map<String, Map<String, Map<String, Map<String, String>>>> readCurrentStateFromFile(String filePath) {
    try {
      // read from file path
      File file = new File(filePath);
      return objectMapper.readValue(file,
          new TypeReference<Map<String, Map<String, Map<String, Map<String, String>>>>>() {
          });
    } catch (IOException e) {
      logger.warn("Failed to read from file: " + filePath);
      return new HashMap<>();
    }
  }

  /**
   * Read instance liveness status from a file, return true if the instance is healthy and the last update time is within timeout.
   * File format: {"IsAlive": true, "LastUpdateTime": 1629300000}
   * @param filePath
   * @param timeoutInSec
   * @return
   */
  public static boolean readInstanceLivenessStatusFromFile(String filePath, int timeoutInSec) {
    try {
      // read from file path
      File file = new File(filePath);
      HostLivenessState status = objectMapper.readValue(file, new TypeReference<HostLivenessState>() {
      });
      return status.isHealthy() && (System.currentTimeMillis()/1000 - status.getLastUpdatedTime()) < timeoutInSec;
    } catch (IOException e) {
      logger.warn("Failed to read from file: " + filePath);
      return false;
    }
  }

  /**
   * Instance health status representation as JSON
   */
  public static class HostLivenessState {
    @JsonProperty ("IsAlive")
    Boolean _isAlive;
    @JsonProperty ("LastUpdateTime")
    long _lastUpdatedTime; // in epoch second

    public Boolean isHealthy(){
      return _isAlive;
    }
    public long getLastUpdatedTime(){
      return _lastUpdatedTime;
    }
  }
}
