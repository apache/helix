package org.apache.helix.zookeeper.datamodel.serializer;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.util.ZNRecordUtil;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZNRecordStreamingSerializer implements ZkSerializer {
  private static Logger LOG = LoggerFactory.getLogger(ZNRecordStreamingSerializer.class);

  private static int getListFieldBound(ZNRecord record) {
    int max = Integer.MAX_VALUE;
    if (record.getSimpleFields().containsKey(ZNRecord.LIST_FIELD_BOUND)) {
      String maxStr = record.getSimpleField(ZNRecord.LIST_FIELD_BOUND);
      try {
        max = Integer.parseInt(maxStr);
      } catch (Exception e) {
        LOG.error("IllegalNumberFormat for list field bound: " + maxStr);
      }
    }
    return max;
  }

  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError {
    if (!(data instanceof ZNRecord)) {
      // null is NOT an instance of any class
      LOG.error("Input object must be of type ZNRecord but it is " + data
          + ". Will not write to zk");
      throw new ZkMarshallingError("Input object is not of type ZNRecord (was " + data + ")");
    }

    // apply retention policy on list field
    ZNRecord record = (ZNRecord) data;
    int max = getListFieldBound(record);
    if (max < Integer.MAX_VALUE) {
      Map<String, List<String>> listMap = record.getListFields();
      for (String key : listMap.keySet()) {
        List<String> list = listMap.get(key);
        if (list.size() > max) {
          listMap.put(key, list.subList(0, max));
        }
      }
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] serializedBytes;
    boolean isCompressed = false;

    try {
      JsonFactory f = new JsonFactory();
      JsonGenerator g = f.createJsonGenerator(baos);

      g.writeStartObject();

      // write id field
      g.writeRaw("\n  ");
      g.writeStringField("id", record.getId());

      // write simepleFields
      g.writeRaw("\n  ");
      g.writeObjectFieldStart("simpleFields");
      for (String key : record.getSimpleFields().keySet()) {
        g.writeRaw("\n    ");
        g.writeStringField(key, record.getSimpleField(key));
      }
      g.writeRaw("\n  ");
      g.writeEndObject(); // for simpleFields

      // write listFields
      g.writeRaw("\n  ");
      g.writeObjectFieldStart("listFields");
      for (String key : record.getListFields().keySet()) {
        // g.writeStringField(key, record.getListField(key).toString());

        // g.writeObjectFieldStart(key);
        g.writeRaw("\n    ");
        g.writeArrayFieldStart(key);
        List<String> list = record.getListField(key);
        for (String listValue : list) {
          g.writeString(listValue);
        }
        // g.writeEndObject();
        g.writeEndArray();

      }
      g.writeRaw("\n  ");
      g.writeEndObject(); // for listFields

      // write mapFields
      g.writeRaw("\n  ");
      g.writeObjectFieldStart("mapFields");
      for (String key : record.getMapFields().keySet()) {
        // g.writeStringField(key, record.getMapField(key).toString());
        g.writeRaw("\n    ");
        g.writeObjectFieldStart(key);
        Map<String, String> map = record.getMapField(key);
        for (String mapKey : map.keySet()) {
          g.writeRaw("\n      ");
          g.writeStringField(mapKey, map.get(mapKey));
        }
        g.writeRaw("\n    ");
        g.writeEndObject();

      }
      g.writeRaw("\n  ");
      g.writeEndObject(); // for mapFields

      byte[] rawPayload = record.getRawPayload();
      if (rawPayload != null && rawPayload.length > 0) {
        // write rawPayload
        g.writeRaw("\n  ");
        g.writeStringField("rawPayload", new String(Base64.encodeBase64(rawPayload), "UTF-8"));
      }

      g.writeRaw("\n");
      g.writeEndObject(); // for whole znrecord

      // important: will force flushing of output, close underlying output
      // stream
      g.close();
      serializedBytes = baos.toByteArray();
      // apply compression if needed
      if (ZNRecordUtil.shouldCompress(record, serializedBytes.length)) {
        serializedBytes = GZipCompressionUtil.compress(serializedBytes);
        isCompressed = true;
      }
    } catch (Exception e) {
      LOG.error(
          "Exception during data serialization. ZNRecord ID: {} will not be written to zk.",
          record.getId(), e);
      throw new ZkMarshallingError(e);
    }
    // check size
    int writeSizeLimit = ZNRecordUtil.getSerializerWriteSizeLimit();
    if (serializedBytes.length > writeSizeLimit) {
      LOG.error("Data size: {} is greater than {} bytes, is compressed: {}, ZNRecord.id: {}."
              + " Data will not be written to Zookeeper.", serializedBytes.length, writeSizeLimit,
          isCompressed, record.getId());
      throw new ZkMarshallingError(
          "Data size: " + serializedBytes.length + " is greater than " + writeSizeLimit
              + " bytes, is compressed: " + isCompressed + ", ZNRecord.id: " + record.getId());
    }

    return serializedBytes;
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError {
    if (bytes == null || bytes.length == 0) {
      LOG.error("ZNode is empty.");
      return null;
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    ZNRecord record = null;
    String id = null;
    Map<String, String> simpleFields = new HashMap<>();
    Map<String, List<String>> listFields = new HashMap<>();
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    byte[] rawPayload = null;

    try {
      // decompress the data if its already compressed
      if (GZipCompressionUtil.isCompressed(bytes)) {
        byte[] uncompressedBytes = GZipCompressionUtil.uncompress(bais);
        bais = new ByteArrayInputStream(uncompressedBytes);
      }
      JsonFactory f = new JsonFactory();
      JsonParser jp = f.createJsonParser(bais);

      jp.nextToken(); // will return JsonToken.START_OBJECT (verify?)
      while (jp.nextToken() != JsonToken.END_OBJECT) {
        String fieldname = jp.getCurrentName();
        jp.nextToken(); // move to value, or START_OBJECT/START_ARRAY
        if ("id".equals(fieldname)) {
          // contains an object
          id = jp.getText();
        } else if ("simpleFields".equals(fieldname)) {
          while (jp.nextToken() != JsonToken.END_OBJECT) {
            String key = jp.getCurrentName();
            jp.nextToken(); // move to value
            simpleFields.put(key, jp.getText());
          }
        } else if ("mapFields".equals(fieldname)) {
          // user.setVerified(jp.getCurrentToken() == JsonToken.VALUE_TRUE);
          while (jp.nextToken() != JsonToken.END_OBJECT) {
            String key = jp.getCurrentName();
            mapFields.put(key, new TreeMap<String, String>());
            jp.nextToken(); // move to value

            while (jp.nextToken() != JsonToken.END_OBJECT) {
              String mapKey = jp.getCurrentName();
              jp.nextToken(); // move to value
              mapFields.get(key).put(mapKey, jp.getText());
            }
          }

        } else if ("listFields".equals(fieldname)) {
          // user.setUserImage(jp.getBinaryValue());
          while (jp.nextToken() != JsonToken.END_OBJECT) {
            String key = jp.getCurrentName();
            listFields.put(key, new ArrayList<String>());
            jp.nextToken(); // move to value
            while (jp.nextToken() != JsonToken.END_ARRAY) {
              listFields.get(key).add(jp.getText());
            }

          }

        } else if ("rawPayload".equals(fieldname)) {
          rawPayload = Base64.decodeBase64(jp.getText());
        } else {
          throw new IllegalStateException("Unrecognized field '" + fieldname + "'!");
        }
      }
      jp.close(); // ensure resources get cleaned up timely and properly

      if (id == null) {
        throw new IllegalStateException("ZNRecord id field is required!");
      }
      record = new ZNRecord(id);
      record.setSimpleFields(simpleFields);
      record.setListFields(listFields);
      record.setMapFields(mapFields);
      record.setRawPayload(rawPayload);
    } catch (Exception e) {
      LOG.error("Exception during deserialization of bytes: " + new String(bytes), e);
    }
    return record;
  }

  public static void main(String[] args) {
    ZNRecord record = new ZNRecord("record");
    final int recordSize = 10;
    for (int i = 0; i < recordSize; i++) {
      record.setSimpleField("" + i, "" + i);
      record.setListField("" + i, new ArrayList<String>());
      for (int j = 0; j < recordSize; j++) {
        record.getListField("" + i).add("" + j);
      }

      record.setMapField("" + i, new TreeMap<String, String>());
      for (int j = 0; j < recordSize; j++) {
        record.getMapField("" + i).put("" + j, "" + j);
      }
    }

    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    byte[] bytes = serializer.serialize(record);
    System.out.println(new String(bytes));
    ZNRecord record2 = (ZNRecord) serializer.deserialize(bytes);
    System.out.println(record2);

    long start = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      bytes = serializer.serialize(record);
      // System.out.println(new String(bytes));
      record2 = (ZNRecord) serializer.deserialize(bytes);
      // System.out.println(record2);
    }
    long end = System.currentTimeMillis();
    System.out.println("ZNRecordStreamingSerializer time used: " + (end - start));

    ZNRecordSerializer serializer2 = new ZNRecordSerializer();
    bytes = serializer2.serialize(record);
    // System.out.println(new String(bytes));
    record2 = (ZNRecord) serializer2.deserialize(bytes);
    // System.out.println(record2);

    start = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      bytes = serializer2.serialize(record);
      // System.out.println(new String(bytes));
      record2 = (ZNRecord) serializer2.deserialize(bytes);
      // System.out.println(record2);
    }
    end = System.currentTimeMillis();
    System.out.println("ZNRecordSerializer time used: " + (end - start));

  }
}
