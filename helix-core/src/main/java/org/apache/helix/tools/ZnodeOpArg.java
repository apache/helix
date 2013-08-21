package org.apache.helix.tools;

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

import java.util.List;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.apache.helix.tools.TestExecutor.ZnodePropertyType;

public class ZnodeOpArg {
  public String _znodePath;
  public ZnodePropertyType _propertyType;

  /**
   * "+" for update/create if not exist
   * '-' for remove
   * "==" for test equals
   * "!=" for test not equal
   */
  public String _operation;
  public String _key;
  public ZnodeValue _updateValue;

  public ZnodeOpArg() {
  }

  /**
   * verify simple/list/map field: no update value
   * @param znodePath
   * @param type
   * @param op
   * @param key
   */
  public ZnodeOpArg(String znodePath, ZnodePropertyType type, String op, String key) {
    this(znodePath, type, op, key, new ZnodeValue());
  }

  /**
   * verify znode: no update value
   * @param znodePath
   * @param type
   * @param op
   */
  public ZnodeOpArg(String znodePath, ZnodePropertyType type, String op) {
    this(znodePath, type, op, null, new ZnodeValue());
  }

  /**
   * simple field change
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param update
   */
  public ZnodeOpArg(String znodePath, ZnodePropertyType type, String op, String key, String update) {
    this(znodePath, type, op, key, new ZnodeValue(update));
  }

  /**
   * list field change
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param update
   */
  public ZnodeOpArg(String znodePath, ZnodePropertyType type, String op, String key,
      List<String> update) {
    this(znodePath, type, op, key, new ZnodeValue(update));
  }

  /**
   * map field change
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param update
   */
  public ZnodeOpArg(String znodePath, ZnodePropertyType type, String op, String key,
      Map<String, String> update) {
    this(znodePath, type, op, key, new ZnodeValue(update));
  }

  /**
   * znode change
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param update
   */
  public ZnodeOpArg(String znodePath, ZnodePropertyType type, String op, ZNRecord update) {
    this(znodePath, type, op, null, new ZnodeValue(update));
  }

  /**
   * @param znodePath
   * @param type
   * @param op
   * @param key
   * @param update
   */
  public ZnodeOpArg(String znodePath, ZnodePropertyType type, String op, String key,
      ZnodeValue update) {
    _znodePath = znodePath;
    _propertyType = type;
    _operation = op;
    _key = key;
    _updateValue = update;
  }

  @Override
  public String toString() {
    String ret =
        "={\"" + _znodePath + "\", " + _propertyType + "/" + _key + " " + _operation + " "
            + _updateValue + "}";
    return ret;
  }

  // TODO temp test; remove it
  /*
   * public static void main(String[] args)
   * {
   * // null modification command
   * ZnodeOpArg command = new ZnodeOpArg();
   * System.out.println(command);
   * // simple modification command
   * command = new ZnodeOpArg("/testPath", ZnodePropertyType.SIMPLE, "+", "key1", "simpleValue1");
   * System.out.println(command);
   * // list modification command
   * List<String> list = new ArrayList<String>();
   * list.add("listValue1");
   * list.add("listValue2");
   * command = new ZnodeOpArg("/testPath", ZnodePropertyType.LIST, "+", "key1", list);
   * System.out.println(command);
   * // map modification command
   * Map<String, String> map = new HashMap<String, String>();
   * map.put("mapKey1", "mapValue1");
   * map.put("mapKey2", "mapValue2");
   * command = new ZnodeOpArg("/testPath", ZnodePropertyType.MAP, "+", "key1", map);
   * System.out.println(command);
   * // map modification command
   * ZNRecord record = new ZNRecord("znrecord");
   * record.setSimpleField("key1", "simpleValue1");
   * record.setListField("key1", list);
   * record.setMapField("key1", map);
   * command = new ZnodeOpArg("/testPath", ZnodePropertyType.ZNODE, "+", record);
   * System.out.println(command);
   * }
   */
}
