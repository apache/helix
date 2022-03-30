package org.apache.helix;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class StringProcessUtil {
  private static final String CONCATENATE_CONFIG_SPLITTER = ",";
  private static final String CONCATENATE_CONFIG_JOINER = "=";


  public static Map<String, String> concatenateConfigParser(String inputStr) {
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

  public static String concatenateMapIntoString(Map<String, String> inputMap) {
    return inputMap
        .entrySet()
        .stream()
        .map(entry -> entry.getKey() + CONCATENATE_CONFIG_JOINER + entry.getValue())
        .collect(Collectors.joining(CONCATENATE_CONFIG_SPLITTER));

  }
}
