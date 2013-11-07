package org.apache.helix.api.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Scope;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

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

/**
 * Generic configuration of Helix components prefixed with a namespace
 */
public class NamespacedConfig extends ZNRecord {
  public static final char PREFIX_CHAR = '!';
  private final String _prefix;

  /**
   * Instantiate a NamespacedConfig. It is intended for use only by entities that can be identified
   * @param scope scope object
   */
  public NamespacedConfig(Scope<?> scope, String prefix) {
    super(scope.getScopedId().stringify());
    _prefix = prefix + PREFIX_CHAR;
  }

  /**
   * Instantiate a NamespacedConfig from an existing HelixProperty
   * @param property property wrapping a configuration
   */
  public NamespacedConfig(HelixProperty property, String prefix) {
    super(property.getRecord());
    _prefix = prefix + PREFIX_CHAR;
    filterNonPrefixedFields();
  }

  /**
   * Instantiate a NamespacedConfig as a copy of another NamedspacedConfig
   * @param config populated NamespacedConfig
   */
  public NamespacedConfig(NamespacedConfig config) {
    super(config.getId());
    _prefix = config.getPrefix() + PREFIX_CHAR;
    if (config.getRawPayload() != null && config.getRawPayload().length > 0) {
      setRawPayload(config.getRawPayload());
      setPayloadSerializer(config.getPayloadSerializer());
    }
    super.setSimpleFields(config.getPrefixedSimpleFields());
    super.setListFields(config.getPrefixedListFields());
    super.setMapFields(config.getPrefixedMapFields());
  }

  @Override
  public void setMapField(String k, Map<String, String> v) {
    super.setMapField(_prefix + k, v);
  }

  @Override
  public Map<String, String> getMapField(String k) {
    return super.getMapField(_prefix + k);
  }

  @Override
  public void setMapFields(Map<String, Map<String, String>> mapFields) {
    for (String k : mapFields.keySet()) {
      super.setMapField(_prefix + k, mapFields.get(k));
    }
  }

  /**
   * Returns an immutable map of map fields
   */
  @Override
  public Map<String, Map<String, String>> getMapFields() {
    return convertToPrefixlessMap(super.getMapFields(), _prefix);
  }

  @Override
  public void setListField(String k, List<String> v) {
    super.setListField(_prefix + k, v);
  }

  @Override
  public List<String> getListField(String k) {
    return super.getListField(_prefix + k);
  }

  @Override
  public void setListFields(Map<String, List<String>> listFields) {
    for (String k : listFields.keySet()) {
      super.setListField(_prefix + k, listFields.get(k));
    }
  }

  /**
   * Returns an immutable map of list fields
   */
  @Override
  public Map<String, List<String>> getListFields() {
    return convertToPrefixlessMap(super.getListFields(), _prefix);
  }

  @Override
  public void setSimpleField(String k, String v) {
    super.setSimpleField(_prefix + k, v);
  }

  @Override
  public String getSimpleField(String k) {
    return super.getSimpleField(_prefix + k);
  }

  @Override
  public void setSimpleFields(Map<String, String> simpleFields) {
    for (String k : simpleFields.keySet()) {
      super.setSimpleField(_prefix + k, simpleFields.get(k));
    }
  }

  /**
   * Returns an immutable map of simple fields
   */
  @Override
  public Map<String, String> getSimpleFields() {
    return convertToPrefixlessMap(super.getSimpleFields(), _prefix);
  }

  /**
   * Get the prefix used to distinguish these config properties
   * @return string prefix, not including the underscore
   */
  public String getPrefix() {
    return _prefix.substring(0, _prefix.indexOf(PREFIX_CHAR));
  }

  /**
   * Remove all fields from this config that are not prefixed
   */
  private void filterNonPrefixedFields() {
    // filter out any configuration that isn't user-defined
    Predicate<String> keyFilter = new Predicate<String>() {
      @Override
      public boolean apply(String key) {
        return key.contains(_prefix);
      }
    };
    super.setMapFields(Maps.filterKeys(super.getMapFields(), keyFilter));
    super.setListFields(Maps.filterKeys(super.getListFields(), keyFilter));
    super.setSimpleFields(Maps.filterKeys(super.getSimpleFields(), keyFilter));
  }

  /**
   * Get all map fields with prefixed keys
   * @return prefixed map fields
   */
  public Map<String, Map<String, String>> getPrefixedMapFields() {
    return super.getMapFields();
  }

  /**
   * Get all list fields with prefixed keys
   * @return prefixed list fields
   */
  public Map<String, List<String>> getPrefixedListFields() {
    return super.getListFields();
  }

  /**
   * Get all simple fields with prefixed keys
   * @return prefixed simple fields
   */
  public Map<String, String> getPrefixedSimpleFields() {
    return super.getSimpleFields();
  }

  /**
   * Add user configuration to an existing helix property.
   * @param property the property to update
   * @param config the user config
   */
  public static void addConfigToProperty(HelixProperty property, NamespacedConfig config) {
    ZNRecord record = property.getRecord();
    record.getMapFields().putAll(config.getPrefixedMapFields());
    record.getListFields().putAll(config.getPrefixedListFields());
    record.getSimpleFields().putAll(config.getPrefixedSimpleFields());
    if (config.getRawPayload() != null && config.getRawPayload().length > 0) {
      record.setPayloadSerializer(config.getPayloadSerializer());
      record.setRawPayload(config.getRawPayload());
    }
  }

  /**
   * Get a copy of a map with the key prefix stripped. The resulting map is immutable
   * @param rawMap map of key, value pairs where the key is prefixed
   * @return map of key, value pairs where the key is not prefixed
   */
  private static <T> Map<String, T> convertToPrefixlessMap(Map<String, T> rawMap, String prefix) {
    Map<String, T> convertedMap = new HashMap<String, T>();
    for (String rawKey : rawMap.keySet()) {
      String k = rawKey.substring(prefix.length());
      convertedMap.put(k, rawMap.get(rawKey));
    }
    return ImmutableMap.copyOf(convertedMap);
  }
}
