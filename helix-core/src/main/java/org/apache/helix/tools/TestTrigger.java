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

public class TestTrigger {
  public long _startTime;
  public long _timeout;
  public ZnodeValue _expectValue;

  /**
   * no time or data trigger
   */
  public TestTrigger() {
    this(0, 0, (ZnodeValue) null);
  }

  /**
   * time trigger with a start time, no data trigger
   * @param startTime
   * @param timeout
   */
  public TestTrigger(long startTime) {
    this(startTime, 0, (ZnodeValue) null);
  }

  /**
   * simple field data trigger
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, String expect) {
    this(startTime, timeout, new ZnodeValue(expect));
  }

  /**
   * list field data trigger
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, List<String> expect) {
    this(startTime, timeout, new ZnodeValue(expect));
  }

  /**
   * map field data trigger
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, Map<String, String> expect) {
    this(startTime, timeout, new ZnodeValue(expect));
  }

  /**
   * znode data trigger
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, ZNRecord expect) {
    this(startTime, timeout, new ZnodeValue(expect));
  }

  /**
   * @param startTime
   * @param timeout
   * @param expect
   */
  public TestTrigger(long startTime, long timeout, ZnodeValue expect) {
    _startTime = startTime;
    _timeout = timeout;
    _expectValue = expect;
  }

  @Override
  public String toString() {
    String ret = "<" + _startTime + "~" + _timeout + "ms, " + _expectValue + ">";
    return ret;
  }

  // TODO temp test; remove it
  /*
   * public static void main(String[] args)
   * {
   * TestTrigger trigger = new TestTrigger(0, 0, "simpleValue0");
   * System.out.println("trigger=" + trigger);
   * List<String> list = new ArrayList<String>();
   * list.add("listValue1");
   * list.add("listValue2");
   * trigger = new TestTrigger(0, 0, list);
   * System.out.println("trigger=" + trigger);
   * Map<String, String> map = new HashMap<String, String>();
   * map.put("mapKey3", "mapValue3");
   * map.put("mapKey4", "mapValue4");
   * trigger = new TestTrigger(0, 0, map);
   * System.out.println("trigger=" + trigger);
   * trigger = new TestTrigger();
   * System.out.println("trigger=" + trigger);
   * }
   */
}
