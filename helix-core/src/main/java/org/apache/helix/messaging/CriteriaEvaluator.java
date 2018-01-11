package org.apache.helix.messaging;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.helix.Criteria;
import org.apache.helix.Criteria.DataSource;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CriteriaEvaluator {
  private static Logger logger = LoggerFactory.getLogger(CriteriaEvaluator.class);
  public static final String MATCH_ALL_SYM = "%";

  /**
   * Examine persisted data to match wildcards in {@link Criteria}
   * @param recipientCriteria Criteria specifying the message destinations
   * @param manager connection to the persisted data
   * @return map of evaluated criteria
   */
  public List<Map<String, String>> evaluateCriteria(Criteria recipientCriteria, HelixManager manager) {
    // get the data
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    List<HelixProperty> properties;
    DataSource dataSource = recipientCriteria.getDataSource();
    String resourceName = recipientCriteria.getResource();
    String instanceName = recipientCriteria.getInstanceName();

    switch (dataSource) {
    case EXTERNALVIEW:
      properties = getProperty(accessor, resourceName, keyBuilder.externalViews(),
          keyBuilder.externalView(resourceName), DataSource.EXTERNALVIEW.name());
      break;
    case IDEALSTATES:
      properties = getProperty(accessor, resourceName, keyBuilder.idealStates(),
          keyBuilder.idealStates(resourceName), DataSource.IDEALSTATES.name());
      break;
    case LIVEINSTANCES:
      properties = getProperty(accessor, instanceName, keyBuilder.liveInstances(),
          keyBuilder.liveInstance(instanceName), DataSource.LIVEINSTANCES.name());
      break;
    case INSTANCES:
      properties = getProperty(accessor, instanceName, keyBuilder.instances(),
          keyBuilder.instance(instanceName), DataSource.INSTANCES.name());
      break;
    default:
      return Lists.newArrayList();
    }
    // flatten the data
    List<ZNRecordRow> allRows = ZNRecordRow.flatten(HelixProperty.convertToList(properties));

    // save the matches
    Set<String> liveParticipants = accessor.getChildValuesMap(keyBuilder.liveInstances()).keySet();
    List<ZNRecordRow> result = Lists.newArrayList();
    for (ZNRecordRow row : allRows) {
      // The participant instance name is stored in the return value of either getRecordId() or getMapSubKey()
      if (rowMatches(recipientCriteria, row) &&
          (liveParticipants.contains(row.getRecordId()) || liveParticipants.contains(row.getMapSubKey()))) {
        result.add(row);
      }
    }

    Set<Map<String, String>> selected = Sets.newHashSet();

    // deduplicate and convert the matches into the required format
    for (ZNRecordRow row : result) {
      Map<String, String> resultRow = new HashMap<String, String>();
      resultRow.put("instanceName", !recipientCriteria.getInstanceName().equals("") ?
          (!Strings.isNullOrEmpty(row.getMapSubKey()) ? row.getMapSubKey() : row.getRecordId()) :
          "");
      resultRow.put("resourceName", !recipientCriteria.getResource().equals("") ? row.getRecordId()
          : "");
      resultRow.put("partitionName", !recipientCriteria.getPartition().equals("") ? row.getMapKey()
          : "");
      resultRow.put("partitionState",
          !recipientCriteria.getPartitionState().equals("") ? row.getMapValue() : "");
      selected.add(resultRow);
    }
    logger.info("Query returned " + selected.size() + " rows");
    return Lists.newArrayList(selected);
  }

  /**
   * Check if a given row matches the specified criteria
   * @param criteria the criteria
   * @param row row of currently persisted data
   * @return true if it matches, false otherwise
   */
  private boolean rowMatches(Criteria criteria, ZNRecordRow row) {
    String instanceName = normalizePattern(criteria.getInstanceName());
    String resourceName = normalizePattern(criteria.getResource());
    String partitionName = normalizePattern(criteria.getPartition());
    String partitionState = normalizePattern(criteria.getPartitionState());
    return (stringMatches(instanceName, Strings.nullToEmpty(row.getMapSubKey())) ||
            stringMatches(instanceName, Strings.nullToEmpty(row.getRecordId())))
        && stringMatches(resourceName, Strings.nullToEmpty(row.getRecordId()))
        && stringMatches(partitionName, Strings.nullToEmpty(row.getMapKey()))
        && stringMatches(partitionState, Strings.nullToEmpty(row.getMapValue()));
  }

  /**
   * Convert an SQL like expression into a Java matches expression
   * @param pattern SQL like match pattern (i.e. contains '%'s and '_'s)
   * @return Java matches expression (i.e. contains ".*?"s and '.'s)
   */
  private String normalizePattern(String pattern) {
    if (pattern == null || pattern.equals("") || pattern.equals("*")) {
      pattern = "%";
    }
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < pattern.length(); i++) {
      char ch = pattern.charAt(i);
      if ("[](){}.*+?$^|#\\".indexOf(ch) != -1) {
        // escape any reserved characters
        builder.append("\\");
      }
      // append the character
      builder.append(ch);
    }
    pattern = builder.toString().toLowerCase().replace("_", ".").replace("%", ".*?");
    return pattern;
  }

  /**
   * Check if a string matches a pattern
   * @param pattern pattern allowed by Java regex matching
   * @param value the string to check
   * @return true if they match, false otherwise
   */
  private boolean stringMatches(String pattern, String value) {
    Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    return p.matcher(value).matches();
  }

  private List<HelixProperty> getProperty(HelixDataAccessor accessor, String dataSpec,
      PropertyKey propertyKeys, PropertyKey propertyKey, String dataType) {
    List<HelixProperty> properties;
    if (Strings.isNullOrEmpty(dataSpec) || dataSpec.equals(MATCH_ALL_SYM)) {
      properties = accessor.getChildValues(propertyKeys);
    } else {
      HelixProperty data = accessor.getProperty(propertyKey);
      if (data == null) {
        throw new HelixException(
            String.format("Specified %s %s is not found!", dataType, dataSpec));
      }
      properties = Collections.singletonList(data);
    }
    return properties;
  }
}
