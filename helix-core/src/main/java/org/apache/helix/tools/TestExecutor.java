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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.PropertyJsonComparator;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.store.PropertyStoreException;
import org.apache.helix.tools.TestCommand.CommandType;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

/**
 * a test is structured logically as a list of commands a command has three parts: COMMAND
 * | TRIGGER | ARG'S COMMAND could be: modify, verify, start, stop
 * TRIGGER is optional and consists of start-time, timeout, and expect-value which means
 * the COMMAND is triggered between [start-time, start-time + timeout] and is triggered
 * when the value in concern equals to expect-value
 * ARG's format depends on COMMAND if COMMAND is modify/verify, arg is in form of:
 * <znode-path, property-type (SIMPLE, LIST, or MAP), operation(+, -, ==, !=), key,
 * update-value> in which key is k1 for SIMPLE, k1|index for LIST, and k1|k2 for MAP field
 * if COMMAND is start/stop, arg is a thread handler
 */

public class TestExecutor {
  /**
   * SIMPLE: simple field change LIST: list field change MAP: map field change ZNODE:
   * entire znode change
   */
  public enum ZnodePropertyType {
    SIMPLE,
    LIST,
    MAP,
    ZNODE
  }

  private enum ZnodeModValueType {
    INVALID,
    SINGLE_VALUE,
    LIST_VALUE,
    MAP_VALUE,
    ZNODE_VALUE
  }

  private static Logger logger = Logger.getLogger(TestExecutor.class);
  private static final long SLEEP_TIME = 500; // in
                                              // ms

  private final static PropertyJsonComparator<String> STRING_COMPARATOR =
      new PropertyJsonComparator<String>(String.class);
  private final static PropertyJsonSerializer<ZNRecord> ZNRECORD_SERIALIZER =
      new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);

  private static ZnodeModValueType getValueType(ZnodePropertyType type, String key) {
    ZnodeModValueType valueType = ZnodeModValueType.INVALID;
    switch (type) {
    case SIMPLE:
      if (key == null) {
        logger.warn("invalid key for simple field: key is null");
      } else {
        String keyParts[] = key.split("/");
        if (keyParts.length != 1) {
          logger.warn("invalid key for simple field: " + key + ", expect 1 part: key1 (no slash)");
        } else {
          valueType = ZnodeModValueType.SINGLE_VALUE;
        }
      }
      break;
    case LIST:
      if (key == null) {
        logger.warn("invalid key for simple field: key is null");
      } else {
        String keyParts[] = key.split("/");
        if (keyParts.length < 1 || keyParts.length > 2) {
          logger.warn("invalid key for list field: " + key
              + ", expect 1 or 2 parts: key1 or key1/index)");
        } else if (keyParts.length == 1) {
          valueType = ZnodeModValueType.LIST_VALUE;
        } else {
          try {
            int index = Integer.parseInt(keyParts[1]);
            if (index < 0) {
              logger.warn("invalid key for list field: " + key + ", index < 0");
            } else {
              valueType = ZnodeModValueType.SINGLE_VALUE;
            }
          } catch (NumberFormatException e) {
            logger.warn("invalid key for list field: " + key + ", part-2 is NOT an integer");
          }
        }
      }
      break;
    case MAP:
      if (key == null) {
        logger.warn("invalid key for simple field: key is null");
      } else {
        String keyParts[] = key.split("/");
        if (keyParts.length < 1 || keyParts.length > 2) {
          logger.warn("invalid key for map field: " + key
              + ", expect 1 or 2 parts: key1 or key1/key2)");
        } else if (keyParts.length == 1) {
          valueType = ZnodeModValueType.MAP_VALUE;
        } else {
          valueType = ZnodeModValueType.SINGLE_VALUE;
        }
      }
      break;
    case ZNODE:
      valueType = ZnodeModValueType.ZNODE_VALUE;
    default:
      break;
    }
    return valueType;
  }

  private static String getSingleValue(ZNRecord record, ZnodePropertyType type, String key) {
    if (record == null || key == null) {
      return null;
    }

    String value = null;
    String keyParts[] = key.split("/");

    switch (type) {
    case SIMPLE:
      value = record.getSimpleField(key);
      break;
    case LIST:
      List<String> list = record.getListField(keyParts[0]);
      if (list == null) {
        logger.warn("invalid key for list field: " + key + ", map for key part-1 doesn't exist");
        return null;
      }
      int idx = Integer.parseInt(keyParts[1]);
      value = list.get(idx);
      break;
    case MAP:
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null) {
        logger.warn("invalid key for map field: " + key + ", map for key part-1 doesn't exist");
        return null;
      }
      value = map.get(keyParts[1]);
      break;
    default:
      break;
    }

    return value;
  }

  private static List<String> getListValue(ZNRecord record, String key) {
    if (record == null) {
      return null;
    }
    return record.getListField(key);
  }

  private static Map<String, String> getMapValue(ZNRecord record, String key) {
    return record.getMapField(key);
  }

  // comparator's for single/list/map-value
  private static boolean compareSingleValue(String actual, String expect, String key, ZNRecord diff) {
    boolean ret = (STRING_COMPARATOR.compare(actual, expect) == 0);

    if (diff != null) {
      diff.setSimpleField(key + "/expect", expect);
      diff.setSimpleField(key + "/actual", actual);
    }
    return ret;
  }

  private static boolean compareListValue(List<String> actualList, List<String> expectList,
      String key, ZNRecord diff) {
    boolean ret = true;
    if (actualList == null && expectList == null) {
      ret = true;
    } else if (actualList == null && expectList != null) {
      ret = false;
      if (diff != null) {
        diff.setListField(key + "/expect", expectList);
      }
    } else if (actualList != null && expectList == null) {
      ret = false;
      if (diff != null) {
        diff.setListField(key + "/actual", actualList);
      }
    } else {
      Iterator<String> itrActual = actualList.iterator();
      Iterator<String> itrExpect = expectList.iterator();
      if (diff != null && diff.getListField(key + "/expect") == null) {
        diff.setListField(key + "/expect", new ArrayList<String>());
      }

      if (diff != null && diff.getListField(key + "/actual") == null) {
        diff.setListField(key + "/actual", new ArrayList<String>());
      }

      while (itrActual.hasNext() && itrExpect.hasNext()) {
        String actual = itrActual.next();
        String expect = itrExpect.next();

        if (STRING_COMPARATOR.compare(actual, expect) != 0) {
          ret = false;
          if (diff != null) {
            diff.getListField(key + "/expect").add(expect);
            diff.getListField(key + "/actual").add(actual);
          }
        }
      }

      while (itrActual.hasNext()) {
        String actual = itrActual.next();
        if (diff != null) {
          diff.getListField(key + "/actual").add(actual);
        }
      }

      while (itrExpect.hasNext()) {
        String expect = itrExpect.next();
        if (diff != null) {
          diff.getListField(key + "/expect").add(expect);
        }
      }
    }
    return ret;
  }

  private static void setMapField(ZNRecord record, String key1, String key2, String value) {
    if (record.getMapField(key1) == null) {
      record.setMapField(key1, new TreeMap<String, String>());
    }
    record.getMapField(key1).put(key2, value);
  }

  private static boolean compareMapValue(Map<String, String> actualMap,
      Map<String, String> expectMap, String mapKey, ZNRecord diff) {
    boolean ret = true;
    if (actualMap == null && expectMap == null) {
      ret = true;
    } else if (actualMap == null && expectMap != null) {
      ret = false;
      if (diff != null) {
        diff.setMapField(mapKey + "/expect", expectMap);
      }
    } else if (actualMap != null && expectMap == null) {
      ret = false;
      if (diff != null) {
        diff.setMapField(mapKey + "/actual", actualMap);
      }

    } else {
      for (String key : actualMap.keySet()) {
        String actual = actualMap.get(key);
        if (!expectMap.containsKey(key)) {
          ret = false;

          if (diff != null) {
            setMapField(diff, mapKey + "/actual", key, actual);
          }
        } else {
          String expect = expectMap.get(key);
          if (STRING_COMPARATOR.compare(actual, expect) != 0) {
            ret = false;
            if (diff != null) {
              setMapField(diff, mapKey + "/actual", key, actual);
              setMapField(diff, mapKey + "/expect", key, expect);
            }
          }
        }
      }

      for (String key : expectMap.keySet()) {
        String expect = expectMap.get(key);
        if (!actualMap.containsKey(key)) {
          ret = false;

          if (diff != null) {
            setMapField(diff, mapKey + "/expect", key, expect);
          }
        } else {
          String actual = actualMap.get(key);
          if (STRING_COMPARATOR.compare(actual, expect) != 0) {
            ret = false;
            if (diff != null) {
              setMapField(diff, mapKey + "/actual", key, actual);
              setMapField(diff, mapKey + "/expect", key, expect);
            }
          }
        }
      }
    }
    return ret;
  }

  private static void setZNRecord(ZNRecord diff, ZNRecord record, String keySuffix) {
    if (diff == null || record == null) {
      return;
    }

    for (String key : record.getSimpleFields().keySet()) {
      diff.setSimpleField(key + "/" + keySuffix, record.getSimpleField(key));
    }

    for (String key : record.getListFields().keySet()) {
      diff.setListField(key + "/" + keySuffix, record.getListField(key));
    }

    for (String key : record.getMapFields().keySet()) {
      diff.setMapField(key + "/" + keySuffix, record.getMapField(key));
    }
  }

  private static boolean compareZnodeValue(ZNRecord actual, ZNRecord expect, ZNRecord diff) {
    boolean ret = true;
    if (actual == null && expect == null) {
      ret = true;
    } else if (actual == null && expect != null) {
      ret = false;
      if (diff != null) {
        setZNRecord(diff, expect, "expect");
      }
    } else if (actual != null && expect == null) {
      ret = false;
      if (diff != null) {
        setZNRecord(diff, actual, "actual");
      }
    } else {
      for (String key : actual.getSimpleFields().keySet()) {
        if (compareSingleValue(actual.getSimpleField(key), expect.getSimpleField(key), key, diff) == false) {
          ret = false;
        }
      }

      for (String key : expect.getMapFields().keySet()) {
        if (!actual.getMapFields().containsKey(key)) {
          if (diff != null) {
            ret = false;
            diff.setMapField(key + "/expect", expect.getMapField(key));
          }
        } else {
          if (compareMapValue(actual.getMapField(key), expect.getMapField(key), key, diff) == false) {
            ret = false;
          }
        }
      }

      for (String key : actual.getMapFields().keySet()) {
        if (!expect.getMapFields().containsKey(key)) {
          if (diff != null) {
            ret = false;
            diff.setMapField(key + "/actual", actual.getMapField(key));
          }
        } else {
          if (compareMapValue(actual.getMapField(key), expect.getMapField(key), key, diff) == false) {
            ret = false;
          }
        }
      }
    }
    return ret;
  }

  private static void resetZNRecord(ZNRecord record) {
    if (record != null) {
      record.getSimpleFields().clear();
      record.getListFields().clear();
      record.getMapFields().clear();
    }
  }

  private static boolean isValueExpected(ZNRecord current, ZnodePropertyType type, String key,
      ZnodeValue expect, ZNRecord diff) {
    // expect value = null means not expect any value
    if (expect == null) {
      return true;
    }

    boolean result = false;
    resetZNRecord(diff);
    ZnodeModValueType valueType = getValueType(type, key);
    switch (valueType) {
    case SINGLE_VALUE:
      String singleValue = getSingleValue(current, type, key);
      result = compareSingleValue(singleValue, expect._singleValue, key, diff);
      break;
    case LIST_VALUE:
      List<String> listValue = getListValue(current, key);
      result = compareListValue(listValue, expect._listValue, key, diff);
      break;
    case MAP_VALUE:
      Map<String, String> mapValue = getMapValue(current, key);
      result = compareMapValue(mapValue, expect._mapValue, key, diff);
      break;
    case ZNODE_VALUE:
      result = compareZnodeValue(current, expect._znodeValue, diff);
      break;
    case INVALID:
      break;
    default:
      break;
    }
    return result;
  }

  private static void setSingleValue(ZNRecord record, ZnodePropertyType type, String key,
      String value) {
    String keyParts[] = key.split("/");

    switch (type) {
    case SIMPLE:
      record.setSimpleField(key, value);
      break;
    case LIST:
      List<String> list = record.getListField(keyParts[0]);
      if (list == null) {
        logger.warn("invalid key for list field: " + key + ", value for key part-1 doesn't exist");
        return;
      }
      int idx = Integer.parseInt(keyParts[1]);
      list.remove(idx);
      list.add(idx, value);
      break;
    case MAP:
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null) {
        logger.warn("invalid key for map field: " + key + ", value for key part-1 doesn't exist");
        return;
      }
      map.put(keyParts[1], value);
      break;
    default:
      break;
    }
  }

  private static void setListValue(ZNRecord record, String key, List<String> value) {
    record.setListField(key, value);
  }

  private static void setMapValue(ZNRecord record, String key, Map<String, String> value) {
    record.setMapField(key, value);
  }

  private static void removeSingleValue(ZNRecord record, ZnodePropertyType type, String key) {
    if (record == null) {
      return;
    }

    String keyParts[] = key.split("/");
    switch (type) {
    case SIMPLE:
      record.getSimpleFields().remove(key);
      break;
    case LIST:
      List<String> list = record.getListField(keyParts[0]);
      if (list == null) {
        logger.warn("invalid key for list field: " + key + ", value for key part-1 doesn't exist");
        return;
      }
      int idx = Integer.parseInt(keyParts[1]);
      list.remove(idx);
      break;
    case MAP:
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null) {
        logger.warn("invalid key for map field: " + key + ", value for key part-1 doesn't exist");
        return;
      }
      map.remove(keyParts[1]);
      break;
    default:
      break;
    }
  }

  private static void removeListValue(ZNRecord record, String key) {
    if (record == null || record.getListFields() == null) {
      record.getListFields().remove(key);
    }
  }

  private static void removeMapValue(ZNRecord record, String key) {
    record.getMapFields().remove(key);
  }

  private static boolean executeVerifier(ZNRecord actual, TestCommand command, ZNRecord diff) {
    final ZnodeOpArg arg = command._znodeOpArg;
    final ZnodeValue expectValue = command._trigger._expectValue;

    boolean result = isValueExpected(actual, arg._propertyType, arg._key, expectValue, diff);
    String operation = arg._operation;
    if (operation.equals("!=")) {
      result = !result;
    } else if (!operation.equals("==")) {
      logger.warn("fail to execute (unsupport operation=" + operation + "):" + operation);
      result = false;
    }

    return result;
  }

  private static boolean compareAndSetZnode(ZnodeValue expect, ZnodeOpArg arg, ZkClient zkClient,
      ZNRecord diff) {
    String path = arg._znodePath;
    ZnodePropertyType type = arg._propertyType;
    String key = arg._key;
    boolean success = true;

    // retry 3 times in case there are write conflicts
    long backoffTime = 20; // ms
    for (int i = 0; i < 3; i++) {
      try {
        Stat stat = new Stat();
        ZNRecord record = zkClient.<ZNRecord> readDataAndStat(path, stat, true);

        if (isValueExpected(record, type, key, expect, diff)) {
          if (arg._operation.compareTo("+") == 0) {
            if (record == null) {
              record = new ZNRecord("default");
            }
            ZnodeModValueType valueType = getValueType(arg._propertyType, arg._key);
            switch (valueType) {
            case SINGLE_VALUE:
              setSingleValue(record, arg._propertyType, arg._key, arg._updateValue._singleValue);
              break;
            case LIST_VALUE:
              setListValue(record, arg._key, arg._updateValue._listValue);
              break;
            case MAP_VALUE:
              setMapValue(record, arg._key, arg._updateValue._mapValue);
              break;
            case ZNODE_VALUE:
              // deep copy
              record =
                  ZNRECORD_SERIALIZER.deserialize(ZNRECORD_SERIALIZER
                      .serialize(arg._updateValue._znodeValue));
              break;
            case INVALID:
              break;
            default:
              break;
            }
          } else if (arg._operation.compareTo("-") == 0) {
            ZnodeModValueType valueType = getValueType(arg._propertyType, arg._key);
            switch (valueType) {
            case SINGLE_VALUE:
              removeSingleValue(record, arg._propertyType, arg._key);
              break;
            case LIST_VALUE:
              removeListValue(record, arg._key);
              break;
            case MAP_VALUE:
              removeMapValue(record, arg._key);
              break;
            case ZNODE_VALUE:
              record = null;
              break;
            case INVALID:
              break;
            default:
              break;
            }
          } else {
            logger.warn("fail to execute (unsupport operation): " + arg._operation);
            success = false;
          }

          if (success == true) {
            if (record == null) {
              zkClient.delete(path);
            } else {
              try {
                zkClient.createPersistent(path, true);
              } catch (ZkNodeExistsException e) {
                // OK
              }
              zkClient.writeData(path, record, stat.getVersion());
            }
            return true;
          } else {
            return false;
          }
        }
      } catch (ZkBadVersionException e) {
        // e.printStackTrace();
      } catch (PropertyStoreException e) {
        // e.printStackTrace();
      }

      try {
        Thread.sleep(backoffTime);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      backoffTime *= 2;
    }

    return false;
  }

  private static class ExecuteCommand implements Runnable {
    private final TestCommand _command;
    private final long _startTime;
    private final ZkClient _zkClient;
    private final CountDownLatch _countDown;
    private final Map<TestCommand, Boolean> _testResults;

    public ExecuteCommand(long startTime, TestCommand command, CountDownLatch countDown,
        ZkClient zkClient, Map<TestCommand, Boolean> testResults) {
      _startTime = startTime;
      _command = command;
      _countDown = countDown;
      _zkClient = zkClient;
      _testResults = testResults;
    }

    @Override
    public void run() {
      boolean result = false;
      long now = System.currentTimeMillis();
      final long timeout = now + _command._trigger._timeout;
      ZNRecord diff = new ZNRecord("diff");
      try {
        if (now < _startTime) {
          Thread.sleep(_startTime - now);
        }

        do {
          if (_command._commandType == CommandType.MODIFY) {
            ZnodeOpArg arg = _command._znodeOpArg;
            final ZnodeValue expectValue = _command._trigger._expectValue;
            result = compareAndSetZnode(expectValue, arg, _zkClient, diff);
            // logger.error("result:" + result + ", " + _command);

            if (result == true) {
              _command._finishTimestamp = System.currentTimeMillis();
              _testResults.put(_command, true);

              break;
            } else {
              // logger.error("result:" + result + ", diff:" + diff);
            }
          } else if (_command._commandType == CommandType.VERIFY) {
            ZnodeOpArg arg = _command._znodeOpArg;
            final String znodePath = arg._znodePath;
            ZNRecord record = _zkClient.<ZNRecord> readData(znodePath, true);

            result = executeVerifier(record, _command, diff);
            // logger.error("result:" + result + ", " + _command.toString());
            if (result == true) {
              _command._finishTimestamp = System.currentTimeMillis();
              _testResults.put(_command, true);
              break;
            } else {
              // logger.error("result:" + result + ", diff:" + diff);
            }
          } else if (_command._commandType == CommandType.START) {
            // TODO add data trigger for START command
            Thread thread = _command._nodeOpArg._thread;
            thread.start();

            result = true;
            _command._finishTimestamp = System.currentTimeMillis();
            logger.info("result:" + result + ", " + _command.toString());
            _testResults.put(_command, true);
            break;
          } else if (_command._commandType == CommandType.STOP) {
            // TODO add data trigger for STOP command
            HelixManager manager = _command._nodeOpArg._manager;
            manager.disconnect();
            Thread thread = _command._nodeOpArg._thread;
            thread.interrupt();

            // System.err.println("stop " +
            // _command._nodeOpArg._manager.getInstanceName());
            result = true;
            _command._finishTimestamp = System.currentTimeMillis();
            logger.info("result:" + result + ", " + _command.toString());
            _testResults.put(_command, true);
            break;
          } else {
            throw new IllegalArgumentException("Unsupport command type (was "
                + _command._commandType + ")");
          }

          Thread.sleep(SLEEP_TIME);

          now = System.currentTimeMillis();
        } while (now <= timeout);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } finally {
        if (result == false) {
          _command._finishTimestamp = System.currentTimeMillis();
          logger.error("result:" + result + ", diff: " + diff);
        }
        _countDown.countDown();
        if (_countDown.getCount() == 0) {
          if (_zkClient != null && _zkClient.getConnection() != null)

          {
            _zkClient.close();
          }
        }
      }
    }
  }

  private static Map<TestCommand, Boolean> executeTestHelper(List<TestCommand> commandList,
      String zkAddr, CountDownLatch countDown) {

    final Map<TestCommand, Boolean> testResults = new ConcurrentHashMap<TestCommand, Boolean>();
    ZkClient zkClient = null;

    zkClient = new ZkClient(zkAddr, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    // sort on trigger's start time, stable sort
    Collections.sort(commandList, new Comparator<TestCommand>() {
      @Override
      public int compare(TestCommand o1, TestCommand o2) {
        return (int) (o1._trigger._startTime - o2._trigger._startTime);
      }
    });

    for (TestCommand command : commandList) {
      testResults.put(command, new Boolean(false));

      TestTrigger trigger = command._trigger;
      command._startTimestamp = System.currentTimeMillis() + trigger._startTime;
      new Thread(new ExecuteCommand(command._startTimestamp, command, countDown, zkClient,
          testResults)).start();
    }

    return testResults;
  }

  public static void executeTestAsync(List<TestCommand> commandList, String zkAddr)
      throws InterruptedException {
    CountDownLatch countDown = new CountDownLatch(commandList.size());
    executeTestHelper(commandList, zkAddr, countDown);
  }

  public static Map<TestCommand, Boolean> executeTest(List<TestCommand> commandList, String zkAddr)
      throws InterruptedException {
    final CountDownLatch countDown = new CountDownLatch(commandList.size());
    Map<TestCommand, Boolean> testResults = executeTestHelper(commandList, zkAddr, countDown);

    // TODO add timeout
    countDown.await();

    return testResults;
  }

}
