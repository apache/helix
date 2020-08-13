package org.apache.helix.controller.changedetector.trimmer;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * An abstract class that contains the common logic to trim HelixProperty by removing unnecessary
 * fields.
 */
public abstract class HelixPropertyTrimmer<T extends HelixProperty> {
  /**
   * The possible Helix-aware field types in the HelixProperty.
   */
  enum FieldType {
    SIMPLE_FIELD, LIST_FIELD, MAP_FIELD
  }

  /**
   * @param property
   * @return a map contains the field keys of all non-trimmable field values that need to be kept.
   */
  protected abstract Map<FieldType, Set<String>> getNonTrimmableFields(T property);

  /**
   * By default, ensure the keys of all map fields and list fields are preserved even after the
   * trim. The keys of list and map fields are relatively stable and contain important information.
   * So they should not be trimmed.
   * Extend to override this behavior if necessary.
   * @param property
   * @return a map contains all non-trimmable field keys that need to be kept.
   *         Note that the values will be trimmed.
   */
  protected Map<FieldType, Set<String>> getNonTrimmableKeys(T property) {
    Map<FieldType, Set<String>> nonTrimmableKeys = new HashMap<>();
    nonTrimmableKeys.put(FieldType.MAP_FIELD, property.getRecord().getMapFields().keySet());
    nonTrimmableKeys.put(FieldType.LIST_FIELD, property.getRecord().getListFields().keySet());
    return nonTrimmableKeys;
  }

  /**
   * @param property
   * @return a copy of the property that has been trimmed.
   */
  public abstract T trimProperty(T property);

  // TODO: Simplify or remove the trim logic when we clearly separate the input and output ZNode.
  // TODO: e.g. Resource Config for user input and the Ideal State for the Helix output.
  /**
   * Return a ZNrecord as the trimmed copy of the original property.
   * Note that we are NOT doing deep copy to avoid performance impact.
   * @param originalProperty
   */
  protected ZNRecord doTrim(T originalProperty) {
    ZNRecord originalZNRecord = originalProperty.getRecord();
    ZNRecord trimmedZNRecord = new ZNRecord(originalProperty.getId());

    // Copy the non-trimmable values to the trimmed record.
    // Note to copy the values first. Or if the key-only copy happens first, the value copy will
    // skip to avoid implicit overwrite.
    copyNonTrimmableInfo(originalZNRecord, trimmedZNRecord, getNonTrimmableFields(originalProperty),
        false);
    // Copy the non-trimmable keys (ignore the values) to the trimmed record.
    copyNonTrimmableInfo(originalZNRecord, trimmedZNRecord, getNonTrimmableKeys(originalProperty),
        true);
    return trimmedZNRecord;
  }

  /**
   * Copy the non trimmable information to the trimmed ZNRecord.
   * @param originalZNRecord
   * @param trimmedZNRecord
   * @param nonTrimmableFields
   * @param trimValue if true, the value will not be copied. Only the keys will be kept.
   */
  private void copyNonTrimmableInfo(ZNRecord originalZNRecord, ZNRecord trimmedZNRecord,
      Map<FieldType, Set<String>> nonTrimmableFields, boolean trimValue) {
    for (Map.Entry<FieldType, Set<String>> fieldEntry : nonTrimmableFields.entrySet()) {
      FieldType fieldType = fieldEntry.getKey();
      Set<String> fieldKeySet = fieldEntry.getValue();
      if (null == fieldKeySet || fieldKeySet.isEmpty()) {
        continue;
      }
      switch (fieldType) {
        case SIMPLE_FIELD:
          fieldKeySet.stream().forEach(fieldKey -> {
            if (originalZNRecord.getSimpleFields().containsKey(fieldKey)) {
              trimmedZNRecord.getSimpleFields().putIfAbsent(fieldKey,
                  trimValue ? null : originalZNRecord.getSimpleField(fieldKey));
            }
          });
          break;
        case LIST_FIELD:
          fieldKeySet.stream().forEach(fieldKey -> {
            if (originalZNRecord.getListFields().containsKey(fieldKey)) {
              trimmedZNRecord.getListFields().putIfAbsent(fieldKey,
                  trimValue ? Collections.EMPTY_LIST : originalZNRecord.getListField(fieldKey));
            }
          });
          break;
        case MAP_FIELD:
          fieldKeySet.stream().forEach(fieldKey -> {
            if (originalZNRecord.getMapFields().containsKey(fieldKey)) {
              trimmedZNRecord.getMapFields().putIfAbsent(fieldKey,
                  trimValue ? Collections.EMPTY_MAP : originalZNRecord.getMapField(fieldKey));
            }
          });
          break;
        default:
          break;
      }
    }
  }
}
