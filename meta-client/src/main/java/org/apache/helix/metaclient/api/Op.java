package org.apache.helix.metaclient.api;

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

/**
 *  Represents a single operation in a multi-operation transaction.  Each operation can be a create, set,
 *  version check or delete operation.
 */
public abstract class Op {
  public enum Type {
    CREATE,
    DELETE,
    SET,
    CHECK
    }

  private String _path;
  private Type _type;

  private Op(Type type, String path) {
    this._type = type;
    this._path = path;
  }
  public static Op create(String path, byte[] data) {
    return new Create(path, data);
  }

  public static Op create(String path, byte[] data, MetaClientInterface.EntryMode createMode) {
    return new Create(path, data, createMode);
  }

  public static Op delete(String path, int version) {
    return new Op.Delete(path, version);
  }

  public static Op set(String path, byte[] data, int version) {
    return new Set(path, data, version);
  }

  public static Op check(String path, int version) {
    return new Check(path, version);
  }

  public Type getType() {
    return this._type;
  }

  public String getPath() {
    return this._path;
  }

  /**
   * Check the version of an entry. True only when the version is the same as expected.
   */
  public static class Check extends Op {
    private final int version;
    public int getVersion() { return version;}
    private Check(String path, int version) {
      super(Type.CHECK, path);
      this.version = version;
    }
  }

  /**
   * Represents a Create operation. Creates a new node.
   */
  public static class Create extends Op {
    protected final byte[] data;
    private MetaClientInterface.EntryMode mode;

    public byte[] getData() {
      return data;
    }
    public MetaClientInterface.EntryMode getEntryMode() {return mode;}

    private Create(String path, byte[] data) {
      super(Type.CREATE, path);
      this.data = data;
    }

    private Create(String path, byte[] data, MetaClientInterface.EntryMode mode) {
      super(Type.CREATE, path);
      this.data = data;
      this.mode = mode;
    }
  }

  /**
   * Represents a Delete operations. Deletes an existing node.
   */
  public static class Delete extends Op{
    private final int version;
    public int getVersion() { return version;}

    private Delete(String path, int version) {
      super(Type.DELETE, path);
      this.version = version;
    }
  }

  /**
   * Represents a Set operation. Sets or updates the data of a node.
   */
  public static class Set extends Op {
    private final byte[] data;
    private final int version;

    public byte[] getData() {
      return data;
    }
    public int getVersion() { return version;}

    private Set(String path, byte[] data, int version) {
      super(Type.SET, path);
      this.data = data;
      this.version = version;
    }
  }
}