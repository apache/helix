package org.apache.helix.zookeeper.api.client;

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

import java.util.List;

import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;


/**
 * A wrapper class of org.apache.zookeeper.Op to support Helix interfaces and monitoring
 * functionality.
 */
public abstract class MultiOp {
  private final String _path;

  private MultiOp(String path) {
    _path = path;
  }

  /**
   * @return the target path of the operation.
   */
  public String getPath() {
    return _path;
  }

  /**
   * Build a new org.apache.zookeeper.Op object for Zookeeper.multi call.
   * @param serializer
   * @return org.apache.zookeeper.Op object corresponding to the MultiOp object.
   */
  public abstract Op buildZkOp(PathBasedZkSerializer serializer);

  /**
   * Return a create MultiOp object.
   * @param path target Zookeeper path
   * @param dataObj optional initial data content of the created node
   * @param createMode CreateMode of the newly created node
   * @return Create MultiOp
   */
  public static MultiOp create(String path, Object dataObj, CreateMode createMode) {
    return new CreateOp(path, dataObj, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
  }

  /**
   * Return a delete MultiOp object.
   * @param path target Zookeeper path
   * @param version optional target version of the deleting node
   * @return Delete MultiOp
   */
  public static MultiOp delete(String path, int version) {
    return new DeleteOp(path, version);
  }

  /**
   * Return a setData MultiOp object.
   * @param path target Zookeeper path
   * @param dataObj data content to be written to the target node
   * @param version optional target version of the modified node
   * @return setData MultiOp
   */
  public static MultiOp setData(String path, Object dataObj, int version) {
    return new SetDataOp(path, dataObj, version);
  }

  /**
   * Create new node operation.
   */
  public static class CreateOp extends MultiOp {
    private final Object _dataObj;
    private final List<ACL> _acl;
    private final CreateMode _createMode;

    private CreateOp(String path, Object dataObj, List<ACL> acl, CreateMode createMode) {
      super(path);
      _dataObj = dataObj;
      _acl = acl;
      _createMode = createMode;
    }

    @Override
    public Op buildZkOp(PathBasedZkSerializer serializer) {
      return Op.create(getPath(), serializer.serialize(_dataObj, getPath()), _acl, _createMode);
    }
  }

  /**
   * Delete node operation.
   */
  public static class DeleteOp extends MultiOp {
    private final int _version;

    private DeleteOp(String path, int version) {
      super(path);
      _version = version;
    }

    @Override
    public Op buildZkOp(PathBasedZkSerializer serializer) {
      return Op.delete(getPath(), _version);
    }
  }

  /**
   * Modify exiting node operation.
   */
  public static class SetDataOp extends MultiOp {
    private final Object _dataObj;
    private final int _version;

    private SetDataOp(String path, Object dataObj, int version) {
      super(path);
      _dataObj = dataObj;
      _version = version;
    }

    @Override
    public Op buildZkOp(PathBasedZkSerializer serializer) {
      return Op.setData(getPath(), serializer.serialize(_dataObj, getPath()), _version);
    }
  }
}
