package org.apache.helix;

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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class HelixConfigStringProcessUtil {
  private static final String CONCATENATE_CONFIG_SPLITTER = ",";
  private static final String CONCATENATE_CONFIG_JOINER = "=";

  /**
   * Parse a string represented map into a map.
   * @param inputStr "propName0=propVal0,propName1=propVal1"
   * @return map {[propName0, propVal0], [propName1, propVal1]}"
   */
  public static Map<String, String> concatenatedConfigParser(String inputStr) {
    Map<String, String> resultMap = new HashMap<>();
    if (inputStr == null || inputStr.isEmpty()) {
      return resultMap;
    }
    String[] pathPairs = inputStr.trim().split(CONCATENATE_CONFIG_SPLITTER);
    for (String pair : pathPairs) {
      String[] values = pair.split(CONCATENATE_CONFIG_JOINER);
      if (values.length != 2 || values[0].isEmpty() || values[1].isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Domain-Value pair %s is not valid.", pair));
      }
      resultMap.put(values[0].trim(), values[1].trim());
    }
    return resultMap;
  }

  /**
   * Concatenate a map into a string .
   * @param inputMap {[propName0, propVal0], [propName1, propVal1]}
   * @return String "propName0=propVal0,propName1=propVal1"
   */
  public static String concatenateMapIntoString(Map<String, String> inputMap) {
    return inputMap
        .entrySet()
        .stream()
        .map(entry -> entry.getKey() + CONCATENATE_CONFIG_JOINER + entry.getValue())
        .collect(Collectors.joining(CONCATENATE_CONFIG_SPLITTER));
  }
}