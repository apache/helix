package org.apache.helix.store;

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

import java.util.Comparator;

import org.apache.log4j.Logger;

public class PropertyJsonComparator<T> implements Comparator<T> {
  static private Logger LOG = Logger.getLogger(PropertyJsonComparator.class);
  private final PropertyJsonSerializer<T> _serializer;

  public PropertyJsonComparator(Class<T> clazz) {
    _serializer = new PropertyJsonSerializer<T>(clazz);
  }

  @Override
  public int compare(T arg0, T arg1) {
    if (arg0 == null && arg1 == null) {
      return 0;
    } else if (arg0 == null && arg1 != null) {
      return -1;
    } else if (arg0 != null && arg1 == null) {
      return 1;
    } else {
      try {
        String s0 = new String(_serializer.serialize(arg0));
        String s1 = new String(_serializer.serialize(arg1));

        return s0.compareTo(s1);
      } catch (PropertyStoreException e) {
        // e.printStackTrace();
        LOG.warn(e.getMessage());
        return -1;
      }
    }
  }

}
