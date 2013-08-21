package org.apache.helix.store.zk;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.data.Stat;

public class ZNode {
  // used for a newly created item, because zkclient.create() doesn't return stat
  // or used for places where we don't care about stat
  public static final Stat ZERO_STAT = new Stat();

  final String _zkPath;
  private Stat _stat;
  Object _data;
  Set<String> _childSet;

  public ZNode(String zkPath, Object data, Stat stat) {
    _zkPath = zkPath;
    _childSet = Collections.<String> emptySet(); // new HashSet<String>();
    _data = data;
    _stat = stat;
  }

  public void removeChild(String child) {
    if (_childSet != Collections.<String> emptySet()) {
      _childSet.remove(child);
    }
  }

  public void addChild(String child) {
    if (_childSet == Collections.<String> emptySet()) {
      _childSet = new HashSet<String>();
    }

    _childSet.add(child);
  }

  public void addChildren(List<String> children) {
    if (children != null && !children.isEmpty()) {
      if (_childSet == Collections.<String> emptySet()) {
        _childSet = new HashSet<String>();
      }

      _childSet.addAll(children);
    }
  }

  public boolean hasChild(String child) {
    return _childSet.contains(child);
  }

  public Set<String> getChildSet() {
    return _childSet;
  }

  public void setData(Object data) {
    // System.out.println("setData: " + _zkPath + ", data: " + data);
    _data = data;
  }

  public Object getData() {
    return _data;
  }

  public void setStat(Stat stat) {
    _stat = stat;
  }

  public Stat getStat() {
    return _stat;
  }

  public void setChildSet(List<String> childNames) {
    if (childNames != null && !childNames.isEmpty()) {
      if (_childSet == Collections.<String> emptySet()) {
        _childSet = new HashSet<String>();
      }

      _childSet.clear();
      _childSet.addAll(childNames);
    }
  }

  @Override
  public String toString() {
    return _zkPath + ", " + _data + ", " + _childSet + ", " + _stat;
  }
}
