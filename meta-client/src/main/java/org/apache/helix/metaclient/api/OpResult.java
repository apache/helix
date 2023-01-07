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
  private int type;

  private OpResult(int type) {
      this.type = type;
  }

  public int getType() {
      return this.type;
  }

  public static class ErrorResult extends OpResult {
    private int err;

    public ErrorResult(int err) {
      super(-1);
      this.err = err;
    }

    public int getErr() {
        return this.err;
    }
  }

  public static class GetDataResult extends OpResult {
    private byte[] data;
    private MetaClientInterface.Stat stat;

    public GetDataResult(byte[] data, MetaClientInterface.Stat stat) {
      super(4);
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

  public static class GetChildrenResult extends OpResult {
    private List<String> children;

    public GetChildrenResult(List<String> children) {
      super(8);
      this.children = children;
    }

    public List<String> getChildren() {
        return this.children;
    }
  }

  public static class CheckResult extends OpResult {
    public CheckResult() {
      super(13);
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof CheckResult)) {
        return false;
      } else {
        CheckResult other = (CheckResult)o;
        return this.getType() == other.getType();
      }
    }

    public int hashCode() {
      return this.getType();
    }
  }

  public static class SetDataResult extends OpResult {
    private MetaClientInterface.Stat stat;

    public SetDataResult(MetaClientInterface.Stat stat) {
      super(5);
      this.stat = stat;
    }

    public MetaClientInterface.Stat getStat() {
      return this.stat;
    }
  }

  public static class DeleteResult extends OpResult {
    public DeleteResult() {
      super(2);
    }
  }

  public static class CreateResult extends OpResult {
    private String path;
    private MetaClientInterface.Stat stat;

    public CreateResult(String path) {
      this(1, path, null);
    }

    public CreateResult(String path, MetaClientInterface.Stat stat) {
      this(15, path, stat);
    }

    private CreateResult(int opcode, String path, MetaClientInterface.Stat stat) {
      super(opcode);
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