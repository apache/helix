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

import java.util.Arrays;
import java.util.List;
/**
 * Represent the result of a single operation of a multi operation transaction.
 */
public class OpResult {

  public enum Type {
    ERRORRESULT,
    GETDATARESULT,
    GETCHILDRENRESULT,
    CHECKRESULT,
    SETDATARESULT,
    DELETERESULT,
    CREATERESULT,
    CREATERESULT_WITH_STAT
  }

  private Type type;

  private OpResult(Type type) {
      this.type = type;
  }

  public Type getType() {
      return this.type;
  }

  /**
   * Represents the result of an operation that was attempted to execute but failed.
   */
  public static class ErrorResult extends OpResult {
    private int err;

    public ErrorResult(int err) {
      super(Type.ERRORRESULT);
      this.err = err;
    }

    public int getErr() {
        return this.err;
    }
  }

  /**
   * Represents the result of a getData() operation.
   */
  public static class GetDataResult extends OpResult {
    private byte[] data;
    private MetaClientInterface.Stat stat;

    public GetDataResult(byte[] data, MetaClientInterface.Stat stat) {
      super(Type.GETDATARESULT);
      this.data = data == null ? null : Arrays.copyOf(data, data.length);
      this.stat = stat;
    }

    public byte[] getData() {
      return this.data == null ? null : Arrays.copyOf(this.data, this.data.length);
    }

    public MetaClientInterface.Stat getStat() {
      return this.stat;
    }
  }

  /**
   * Represents the result of a getChildren() operation.
   */
  public static class GetChildrenResult extends OpResult {
    private List<String> children;

    public GetChildrenResult(List<String> children) {
      super(Type.GETCHILDRENRESULT);
      this.children = children;
    }

    public List<String> getChildren() {
        return this.children;
    }
  }

  /**
   * Represents the result of a check() operation.
   */
  public static class CheckResult extends OpResult {
    public CheckResult() {
      super(Type.CHECKRESULT);
    }
  }

  /**
   * Represents the result of a set() operation.
   */
  public static class SetDataResult extends OpResult {
    private MetaClientInterface.Stat stat;

    public SetDataResult(MetaClientInterface.Stat stat) {
      super(Type.SETDATARESULT);
      this.stat = stat;
    }

    public MetaClientInterface.Stat getStat() {
      return this.stat;
    }
  }

  /**
   * Represents the result of a delete() operation.
   */
  public static class DeleteResult extends OpResult {
    public DeleteResult() {
      super(Type.DELETERESULT);
    }
  }

  /**
   * Represents the result of a create() operation.
   */
  public static class CreateResult extends OpResult {
    private String path;
    private MetaClientInterface.Stat stat;

    public CreateResult(String path) {
      this(Type.CREATERESULT, path, null);
    }

    public CreateResult(String path, MetaClientInterface.Stat stat) {
      this(Type.CREATERESULT_WITH_STAT, path, stat);
    }

    private CreateResult(Type type, String path, MetaClientInterface.Stat stat) {
      super(type);
      this.path = path;
      this.stat = stat;
    }

    public String getPath() {
        return this.path;
    }

    public MetaClientInterface.Stat getStat() {
        return this.stat;
    }
  }
}