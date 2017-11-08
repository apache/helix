package org.apache.helix.util;

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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;


public class TestInputLoader {
  public static Object[][] loadTestInputs(String inputFile, String[] params) {
    List<Object[]> data = new ArrayList<Object[]>();
    InputStream inputStream = TestInputLoader.class.getClassLoader().getResourceAsStream(inputFile);
    try {
      ObjectReader mapReader = new ObjectMapper().reader(Map[].class);
      Map[] inputs = mapReader.readValue(inputStream);

      for (Map input : inputs) {
        Object[] objects = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
          objects[i] = input.get(params[i]);
        }
        data.add(objects);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    Object[][] ret = new Object[data.size()][];
    for(int i = 0; i < data.size(); i++) {
      ret[i] = data.get(i);
    }
    return ret;
  }
}
