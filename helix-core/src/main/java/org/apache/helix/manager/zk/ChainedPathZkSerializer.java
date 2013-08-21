package org.apache.helix.manager.zk;

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
import java.util.List;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class ChainedPathZkSerializer implements PathBasedZkSerializer {

  public static class Builder {
    private final ZkSerializer _defaultSerializer;
    private List<ChainItem> _items = new ArrayList<ChainItem>();

    private Builder(ZkSerializer defaultSerializer) {
      _defaultSerializer = defaultSerializer;
    }

    /**
     * Add a serializing strategy for the given path prefix
     * The most specific path will triumph over a more generic (shorter)
     * one regardless of the ordering of the calls.
     */
    public Builder serialize(String path, ZkSerializer withSerializer) {
      _items.add(new ChainItem(normalize(path), withSerializer));
      return this;
    }

    /**
     * Builds the serializer with the given strategies and default serializer.
     */
    public ChainedPathZkSerializer build() {
      return new ChainedPathZkSerializer(_defaultSerializer, _items);
    }
  }

  /**
   * Create a builder that will use the given serializer by default
   * if no other strategy is given to solve the path in question.
   */
  public static Builder builder(ZkSerializer defaultSerializer) {
    return new Builder(defaultSerializer);
  }

  private final List<ChainItem> _items;
  private final ZkSerializer _defaultSerializer;

  private ChainedPathZkSerializer(ZkSerializer defaultSerializer, List<ChainItem> items) {
    _items = items;
    // sort by longest paths first
    // if two items would match one would be prefix of the other
    // and the longest must be more specific
    Collections.sort(_items);
    _defaultSerializer = defaultSerializer;
  }

  @Override
  public byte[] serialize(Object data, String path) throws ZkMarshallingError {
    for (ChainItem item : _items) {
      if (item.matches(path))
        return item._serializer.serialize(data);
    }
    return _defaultSerializer.serialize(data);
  }

  @Override
  public Object deserialize(byte[] bytes, String path) throws ZkMarshallingError {
    for (ChainItem item : _items) {
      if (item.matches(path))
        return item._serializer.deserialize(bytes);
    }
    return _defaultSerializer.deserialize(bytes);
  }

  private static class ChainItem implements Comparable<ChainItem> {
    final String _path;
    final ZkSerializer _serializer;

    ChainItem(String path, ZkSerializer serializer) {
      _path = path;
      _serializer = serializer;
    }

    boolean matches(String path) {
      if (_path.equals(path)) {
        return true;
      } else if (path.length() > _path.length()) {
        if (path.startsWith(_path) && path.charAt(_path.length()) == '/') {
          return true;
        }
      }
      return false;
    }

    @Override
    public int compareTo(ChainItem o) {
      return o._path.length() - _path.length();
    }
  }

  private static String normalize(String path) {
    if (!path.startsWith("/")) {
      // ensure leading slash
      path = "/" + path;
    }
    if (path.endsWith("/")) {
      // remove trailing slash
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

}
