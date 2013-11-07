package org.apache.helix.alerts;

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
import java.util.Iterator;
import java.util.List;

public class Tuple<T> {
  List<T> elements;

  public Tuple() {
    elements = new ArrayList<T>();
  }

  public int size() {
    return elements.size();
  }

  public void add(T entry) {
    elements.add(entry);
  }

  public void addAll(Tuple<T> incoming) {
    elements.addAll(incoming.getElements());
  }

  public Iterator<T> iterator() {
    return elements.listIterator();
  }

  public T getElement(int ind) {
    return elements.get(ind);
  }

  public List<T> getElements() {
    return elements;
  }

  public void clear() {
    elements.clear();
  }

  public static Tuple<String> fromString(String in) {
    Tuple<String> tup = new Tuple<String>();
    if (in.length() > 0) {
      String[] elements = in.split(",");
      for (String element : elements) {
        tup.add(element);
      }
    }
    return tup;
  }

  public String toString() {
    StringBuilder out = new StringBuilder();
    Iterator<T> it = iterator();
    boolean outEmpty = true;
    while (it.hasNext()) {
      if (!outEmpty) {
        out.append(",");
      }
      out.append(it.next());
      outEmpty = false;
    }
    return out.toString();
  }
}
