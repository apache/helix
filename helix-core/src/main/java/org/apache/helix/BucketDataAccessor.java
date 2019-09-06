package org.apache.helix;
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

public interface BucketDataAccessor {

  /**
   * Write a HelixProperty in buckets, compressed.
   * @param path path to which the metadata will be written to
   * @param value HelixProperty to write
   * @param <T>
   * @throws IOException
   */
  <T extends HelixProperty> boolean compressedBucketWrite(String path, T value) throws IOException;

  /**
   * Read a HelixProperty that was written in buckets, compressed.
   * @param path
   * @param helixPropertySubType the subtype of HelixProperty the data was written in
   * @param <T>
   */
  <T extends HelixProperty> HelixProperty compressedBucketRead(String path,
      Class<T> helixPropertySubType);

  /**
   * Delete the HelixProperty in the given path.
   * @param path
   */
  void compressedBucketDelete(String path);

  /**
   * Close the connection to the metadata store.
   */
  void disconnect();
}
