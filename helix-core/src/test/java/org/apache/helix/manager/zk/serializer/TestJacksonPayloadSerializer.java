package org.apache.helix.manager.zk.serializer;

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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZNRecordStreamingSerializer;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestJacksonPayloadSerializer {
  /**
   * Ensure that the JacksonPayloadSerializer can serialize and deserialize arbitrary objects
   */
  @Test
  public void testJacksonSerializeDeserialize() {
    final String RECORD_ID = "testJacksonSerializeDeserialize";
    SampleDeserialized sample = getSample();
    ZNRecord znRecord = new ZNRecord(RECORD_ID);
    znRecord.setPayloadSerializer(new JacksonPayloadSerializer());
    znRecord.setPayload(sample);
    SampleDeserialized duplicate = znRecord.getPayload(SampleDeserialized.class);
    Assert.assertEquals(duplicate, sample);
  }

  /**
   * Test that the payload can be deserialized after serializing and deserializing the ZNRecord
   * that encloses it. This uses ZNRecordSerializer.
   */
  @Test
  public void testFullZNRecordSerializeDeserialize() {
    final String RECORD_ID = "testFullZNRecordSerializeDeserialize";
    SampleDeserialized sample = getSample();
    ZNRecord znRecord = new ZNRecord(RECORD_ID);
    znRecord.setPayloadSerializer(new JacksonPayloadSerializer());
    znRecord.setPayload(sample);
    ZNRecordSerializer znRecordSerializer = new ZNRecordSerializer();
    byte[] serialized = znRecordSerializer.serialize(znRecord);
    ZNRecord deserialized = (ZNRecord) znRecordSerializer.deserialize(serialized);
    deserialized.setPayloadSerializer(new JacksonPayloadSerializer());
    SampleDeserialized duplicate = deserialized.getPayload(SampleDeserialized.class);
    Assert.assertEquals(duplicate, sample);
  }

  /**
   * Test that the payload can be deserialized after serializing and deserializing the ZNRecord
   * that encloses it. This uses ZNRecordStreamingSerializer.
   */
  @Test
  public void testFullZNRecordStreamingSerializeDeserialize() {
    final String RECORD_ID = "testFullZNRecordStreamingSerializeDeserialize";
    SampleDeserialized sample = getSample();
    ZNRecord znRecord = new ZNRecord(RECORD_ID);
    znRecord.setPayloadSerializer(new JacksonPayloadSerializer());
    znRecord.setPayload(sample);
    ZNRecordStreamingSerializer znRecordSerializer = new ZNRecordStreamingSerializer();
    byte[] serialized = znRecordSerializer.serialize(znRecord);
    ZNRecord deserialized = (ZNRecord) znRecordSerializer.deserialize(serialized);
    deserialized.setPayloadSerializer(new JacksonPayloadSerializer());
    SampleDeserialized duplicate = deserialized.getPayload(SampleDeserialized.class);
    Assert.assertEquals(duplicate, sample);
  }

  /**
   * Test that the payload is not included whenever it is not null. This is mainly to maintain
   * backward
   * compatibility.
   */
  @Test
  public void testRawPayloadMissingIfUnspecified() {
    final String RECORD_ID = "testRawPayloadMissingIfUnspecified";
    ZNRecord znRecord = new ZNRecord(RECORD_ID);
    ZNRecordSerializer znRecordSerializer = new ZNRecordSerializer();
    byte[] serialized = znRecordSerializer.serialize(znRecord);
    ZNRecordStreamingSerializer znRecordStreamingSerializer = new ZNRecordStreamingSerializer();
    byte[] streamingSerialized = znRecordStreamingSerializer.serialize(znRecord);
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode jsonNode = mapper.readTree(new String(serialized));
      Assert.assertFalse(jsonNode.has("rawPayload"));
      JsonNode streamingJsonNode = mapper.readTree(new String(streamingSerialized));
      Assert.assertFalse(streamingJsonNode.has("rawPayload"));
    } catch (JsonProcessingException e) {
      Assert.fail();
    } catch (IOException e) {
      Assert.fail();
    }
  }

  /**
   * Get an object which can be tested for serialization success or failure
   * @return Initialized SampleDeserialized object
   */
  private SampleDeserialized getSample() {
    final int INT_FIELD_VALUE = 12345;
    final int LIST_FIELD_COUNT = 5;
    List<Integer> intList = new LinkedList<Integer>();
    for (int i = 0; i < LIST_FIELD_COUNT; i++) {
      intList.add(i);
    }
    return new SampleDeserialized(INT_FIELD_VALUE, intList);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SampleDeserialized {
    private int _intField;
    private List<Integer> _listField;

    public SampleDeserialized() {
    }

    public SampleDeserialized(int intField, List<Integer> listField) {
      _intField = intField;
      _listField = listField;
    }

    @JsonProperty
    public void setIntField(int value) {
      _intField = value;
    }

    @JsonProperty
    public int getIntField() {
      return _intField;
    }

    @JsonProperty
    public void setListField(final List<Integer> listField) {
      _listField = listField;
    }

    @JsonProperty
    public List<Integer> getListField() {
      return _listField;
    }

    @Override
    public boolean equals(Object other) {
      boolean result = true;
      if (other instanceof SampleDeserialized) {
        SampleDeserialized that = (SampleDeserialized) other;
        if (_intField != that._intField) {
          // ints must match
          result = false;
        } else if (_listField != null) {
          // lists must match if one is not null
          if (!_listField.equals(that._listField)) {
            result = false;
          }
        } else {
          // both must be null if one is null
          if (that._listField != null) {
            result = false;
          }
        }
      } else {
        // only compare objects of the same type
        result = false;
      }
      return result;
    }
  }

}
